import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import pvlib
import dotenv

dotenv.load_dotenv()

from pandas import DataFrame, Series, DatetimeIndex, Period

from wondergrid.datasets.era5 import ERA5Dataset, ERA5DatasetBuilder
from wondergrid.datasets.smartmeter import SmartMeterDataset

class IrradianceModel():

    def __init__(self, latitude: float, longitude: float):
        self.location = pvlib.location.Location(latitude, longitude)

    def simulate_irradiance(self, timeline: DatetimeIndex) -> DataFrame:
        raise NotImplementedError()


class ClearSkyIrradianceModel(IrradianceModel):

    def __init__(self, latitude: float, longitude: float):
        super().__init__(latitude, longitude)

    def simulate_irradiance(self, timeline: DatetimeIndex) -> DataFrame:
        solarposition = self.location.get_solarposition(timeline)
        irradiance = self.location.get_clearsky(timeline)
        return (solarposition, irradiance)

class ERA5IrradianceModel(IrradianceModel):

    def __init__(self, latitude: float, longitude: float, weatherdata: DataFrame):
        super().__init__(latitude, longitude)
        self.weatherdata = weatherdata

    def simulate_irradiance(self, timeline: DatetimeIndex) -> DataFrame:
        solarposition = self.location.get_solarposition(timeline)
        weather = self.weatherdata.loc[timeline]
        ghi = weather['ssrd'] / 3600
        dni = weather['fdir'] / 3600
        irradiance = pvlib.irradiance.complete_irradiance(solar_zenith=solarposition['apparent_zenith'], ghi=ghi, dni=dni)
        return (solarposition, irradiance)


class RMIIrradianceModel(IrradianceModel):

    def __init__(self, latitude: float, longitude: float, weatherdata: DataFrame):
        super().__init__(latitude, longitude)
        self.weatherdata = weatherdata

    def simulate_solarposition_irradiance(self, timeline: DatetimeIndex):
        solarposition = self.location.get_solarposition(timeline)
        weather = self.weatherdata.loc[timeline]
        ghi = weather['short_wave_from_sky_tot']
        dhi = weather['short_wave_from_sky_tot'] - weather['sun_int_horiz_tot']
        irradiance = pvlib.irradiance.complete_irradiance(solar_zenith=solarposition['apparent_zenith'], ghi=ghi, dhi=dhi)
        return (solarposition, irradiance)

class WeatherModel():

    def __init__(self):
        pass

    def simulate_temperature(self, timeline: DatetimeIndex) -> DataFrame:
        raise NotImplementedError()


class ERA5WeatherModel(WeatherModel):

    def __init__(self, weatherdata: DataFrame):
        super().__init__()
        self.weatherdata = weatherdata

    def simulate_temperature(self, timeline: DatetimeIndex) -> DataFrame:
        return self.weatherdata.loc[timeline]['t2m']


class PVSystemModel:
    
    def __init__(self, latitude: float, longitude: float, irradiancemodel: IrradianceModel, **params):
        self.location = pvlib.location.Location(latitude, longitude)
        self.irradiancemodel = irradiancemodel
        self.params = params

    @property
    def surface_tilt(self) -> float:
        return self.params.get('surface_tilt', 0.0)
    
    @surface_tilt.setter
    def surface_tilt(self, value: float) -> None:
        self.params['surface_tilt'] = value
    
    @property
    def surface_azimuth(self) -> float:
        return self.params.get('surface_azimuth', 0.0)
    
    @surface_azimuth.setter
    def surface_azimuth(self, value: float) -> None:
        self.params['surface_azimuth'] = value

    @property
    def efficiency_size(self) -> float:
        return self.params.get('efficiency_size', 1.0)
    
    @efficiency_size.setter
    def efficiency_size(self, value: float) -> None:
        self.params['efficiency_size'] = value

    def simulate_pv_power(self, timeline: DatetimeIndex) -> Series:
        raise NotImplementedError()


class SimplePVSystemModel(PVSystemModel):

    def simulate_pv_power(self, timeline: DatetimeIndex) -> Series:
        solarposition, irradiance = self.irradiancemodel.simulate_irradiance(timeline)

        poa_irradiance = pvlib.irradiance.get_total_irradiance(
            surface_tilt=self.surface_tilt,
            surface_azimuth=self.surface_azimuth,
            solar_zenith=solarposition['apparent_zenith'],
            solar_azimuth=solarposition['azimuth'],
            dni=irradiance['dni'],
            ghi=irradiance['ghi'],
            dhi=irradiance['dhi'],
            model='isotropic'
        )

        pv_power_simulated = poa_irradiance['poa_global'] * self.efficiency_size

        return pv_power_simulated


class SimplePVSystemModelWithTemperature(SimplePVSystemModel):

    def __init__(self, latitude: float, longitude: float, irradiancemodel: IrradianceModel, weathermodel: WeatherModel, **params):
        super().__init__(latitude, longitude, irradiancemodel, **params)
        self.weathermodel = weathermodel
        
    @property
    def temp_coefficient(self) -> float:
        return self.params.get('temp_coefficient')
    
    @temp_coefficient.setter
    def temp_coefficient(self, value: float) -> None:
        self.params['temp_coefficient'] = value

    @property
    def temp_baseline(self) -> float:
        return self.params.get('temp_baseline')
    
    @temp_baseline.setter
    def temp_baseline(self, value: float) -> None:
        self.params['temp_baseline'] = value

    def simulate_pv_power(self, timeline: DatetimeIndex) -> Series:
        temperature = self.weathermodel.simulate_temperature(timeline)
        temp_factor = (1.0 + self.temp_coefficient * (self.temp_baseline - temperature))
        pv_power_simulated = super().simulate_pv_power(timeline) * temp_factor
        return pv_power_simulated


def fit_pv_parameters_main(pvmodel: SimplePVSystemModel, profile: DataFrame, verbose: bool = False):
    results = []

    lower_tolerance = 0.1
    upper_tolerance = 0.1

    pv_power_simulated_base = pvmodel.simulate_pv_power(profile.index).max()
    feedin_power_measured_max = profile['feedin'].max()
    efficiency_range = feedin_power_measured_max / pv_power_simulated_base * 3

    for efficiency_size in np.linspace(0, efficiency_range, 5):
        for surface_azimuth in np.linspace(0, 360, 7):
            for surface_tilt in np.linspace(0, 90, 7):

                pvmodel.efficiency_size = efficiency_size
                pvmodel.surface_azimuth = surface_azimuth
                pvmodel.surface_tilt = surface_tilt
                
                pv_power_simulated = pvmodel.simulate_pv_power(profile.index)

                error = np.sqrt(np.square(np.subtract(profile['feedin'], pv_power_simulated)).mean())

                pv_power_simulated_max = pv_power_simulated.resample('24h', origin='start').transform('max')

                check_lower_threshold = (pv_power_simulated < lower_tolerance * pv_power_simulated_max)
                check_upper_threshold = (pv_power_simulated + upper_tolerance * pv_power_simulated_max > profile['feedin'])

                valid = np.all(check_lower_threshold | check_upper_threshold)

                results.append((pvmodel.params.copy(), pv_power_simulated, error, valid))

                if verbose:
                    print(f"eval_main:\tk={efficiency_size:.2%}\tazimuth={surface_azimuth}°\ttilt={surface_tilt}°\tpv_power_simulated={pv_power_simulated.mean():.2f}W\tfeedin_power_measured={profile['feedin'].mean():.2f}W\terror={error:.2f}W\tvalid={valid}")

    best_valid_result = min(filter(lambda result: result[3], results), key=lambda result: result[2], default=None)

    return results, best_valid_result


def fit_pv_parameters_temperature(pvmodel: SimplePVSystemModelWithTemperature, profile: DataFrame, verbose: bool = False):
    results = []

    lower_tolerance = 0.1
    upper_tolerance = 0.1

    for temp_coefficient in np.arange(0.003, 0.007, 0.0001):

        pvmodel.temp_coefficient = temp_coefficient

        pv_power_simulated = pvmodel.simulate_pv_power(profile.index)

        error = np.sqrt(np.square(np.subtract(profile['feedin'], pv_power_simulated)).mean())

        pv_power_simulated_max = pv_power_simulated.resample('24h', origin='start').transform('max')

        check_lower_threshold = (pv_power_simulated < lower_tolerance * pv_power_simulated_max)
        check_upper_threshold = (pv_power_simulated + upper_tolerance * pv_power_simulated_max > profile['feedin'])

        valid = np.all(check_lower_threshold | check_upper_threshold)

        results.append((pvmodel.params, pv_power_simulated, error, valid))

        if verbose:
            print(f"eval_temp:\ttemp_baseline={pvmodel.params['temp_baseline']:.2f}°C\ttemp_coefficient={pvmodel.params['temp_coefficient']:.2%}\tpv_power_simulated={pv_power_simulated.mean():.2f}W\tfeedin_power_measured={profile['feedin'].mean():.2f}W\terror={error:.2f}W\tvalid={valid}")

    best_valid_result = min(filter(lambda result: result[3], results), key=lambda result: result[2], default=None)

    return results, best_valid_result


def plot_results_for_day(ax, results, profile: DataFrame, period: Period, show_valid_results = False, show_invalid_results = False):
    valid_results = list(filter(lambda result: result[3], results))
    invalid_results = list(filter(lambda result: not result[3], results))

    ax.clear()

    ax.set_xlim(0, 24)
    ax.set_ylim(0, 100000)

    if show_invalid_results:
        lines_invalid = [list(enumerate(pv_power_simulated.loc[period.start_time:period.end_time])) for _, pv_power_simulated, _, _ in invalid_results]
        line_collection_invalid = mpl.collections.LineCollection(lines_invalid, array=list(range(len(lines_invalid))), color='grey')
        ax.add_collection(line_collection_invalid)

    if show_valid_results:
        lines_valid = [list(enumerate(pv_power_simulated.loc[period.start_time:period.end_time])) for _, pv_power_simulated, _, _ in valid_results]
        values_valid = [error for _, _, error, _ in valid_results]
        line_collection_valid = mpl.collections.LineCollection(lines_valid, array=values_valid, cmap='viridis')
        ax.add_collection(line_collection_valid)

    ax.plot(list(profile['feedin'].loc[period.start_time:period.end_time]), label='feedin_power_measured', color='black')
    ax.plot(list(profile['production'].loc[period.start_time:period.end_time]), label='pv_power_measured', color='red')

    best_valid_result = min(valid_results, key=lambda result: result[2], default=None)

    if best_valid_result:

        _, pv_power_simulated_best, _, _ = best_valid_result

        ax.plot(list(pv_power_simulated_best.loc[period.start_time:period.end_time]), label='pv_power_simulated_best', color='red', linestyle='dashed')
    
    # best_valid_result_2 = self.results_2.get_best_valid_result()

    # if best_valid_result_2:

    #     best_params, best_pv_power_simulated, _ = best_valid_result_2

    #     ax.plot(list(best_pv_power_simulated.loc[period.start_time:period.end_time]), label='best_pv_power_simulated_with_temp', color='red', linestyle='dotted')

    #     best_pv_power_simulated_with_weather_era5 = self.pv.simulate_pv_power_weather_era5(best_params)
    #     ax.plot(list(best_pv_power_simulated_with_weather_era5.loc[period.start_time:period.end_time]), label='best_pv_power_simulated_with_weather_era5', color='orange', linestyle='solid')

    #     best_pv_power_simulated_with_weather_swfs = self.pv.simulate_pv_power_weather_swfs(best_params)
    #     ax.plot(list(best_pv_power_simulated_with_weather_swfs.loc[period.start_time:period.end_time]), label='best_pv_power_simulated_with_weather_swfs', color='brown', linestyle='solid')

    #     max_generation_point = best_pv_power_simulated.loc[period.start_time:period.end_time].idxmax()
    #     ax.axvline(max_generation_point.hour, label='max_generation_point', color='black')
    
    ax.legend()
    ax.set_xlabel('Hour')
    ax.set_ylabel('Power [W]')

def plot_results(results, profile: DataFrame, show_valid_results=False, show_invalid_results=False):
    periods = pd.period_range(start=profile.index.min(), end=profile.index.max(), freq='24h')

    initial_day = 0

    fig, ax = plt.subplots()

    plt.subplots_adjust(bottom=0.25)

    ax_day = plt.axes([ax.get_position().x0, ax.get_position().y0 - 0.20, ax.get_position().width, 0.05])
    slider_day = mpl.widgets.Slider(ax_day, 'Day', 0, len(periods) - 1, valinit=initial_day, valstep=1)
    slider_day.on_changed(lambda day: plot_results_for_day(ax, results, profile, periods[day], show_valid_results, show_invalid_results))

    # fig.canvas.draw_idle()

    plot_results_for_day(ax, results, profile, periods[initial_day], show_valid_results, show_invalid_results)

    plt.show()


def compare_pv_models(pvmodels: list[PVSystemModel], profile: DataFrame):
    results = []

    for pvmodel in pvmodels:
        pv_power_simulated = pvmodel.simulate_pv_power(profile.index)
        # error_feedin = np.sqrt(np.square(np.subtract(profile['feedin'], pv_power_simulated)).mean())
        error_production = np.sqrt(np.square(np.subtract(profile['production'], pv_power_simulated)).mean())
        results.append((pvmodel.irradiancemodel.__class__.__name__, pv_power_simulated, error_production))

    periods = pd.period_range(start=profile.index.min(), end=profile.index.max(), freq='24h')

    errors = [error for _, _, error in results]

    cmap = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=min(errors), vmax=max(errors)), cmap=mpl.colormaps['viridis'])

    initial_day = 0

    fig, ax = plt.subplots()

    plt.subplots_adjust(bottom=0.25)

    def update(ax, period: Period):
        ax.clear()
        for name, pv_power_simulated, error in results:
            ax.plot(list(pv_power_simulated.loc[period.start_time:period.end_time]), color=cmap.to_rgba(error), label=f'{name} (error: {error})')
        ax.plot(list(profile['feedin'].loc[period.start_time:period.end_time]), label='feedin_power_measured', color='black')
        ax.plot(list(profile['production'].loc[period.start_time:period.end_time]), label='pv_power_measured', color='red')
        ax.legend()
        ax.set_xlabel('Hour')
        ax.set_ylabel('Power [W]')

    ax_day = plt.axes([ax.get_position().x0, ax.get_position().y0 - 0.20, ax.get_position().width, 0.05])
    slider_day = mpl.widgets.Slider(ax_day, 'Day', 0, len(periods) - 1, valinit=initial_day, valstep=1)
    slider_day.on_changed(lambda day: update(ax, periods[day]))

    update(ax, periods[initial_day])

    # fig.colorbar(cmap, label='error', ax=ax)

    plt.show()


def estimate_pv_production(smartmeterdataset: SmartMeterDataset, era5dataset: ERA5Dataset, use_temperature_optimization):

    for (id, profile, metadata) in smartmeterdataset.get_profiles():
        
        era5data = era5dataset.get_weather_for_location(metadata['location'])

        profile = profile * 1000 # from kilowatt to watt

        latitude = metadata['latitude']
        longitude = metadata['longitude']

        cls_irradiancemodel = ClearSkyIrradianceModel(latitude, longitude)

        cls_pvmodel = SimplePVSystemModel(latitude, longitude, cls_irradiancemodel)

        results, best_valid_result = fit_pv_parameters_main(cls_pvmodel, profile, verbose=True)

        if best_valid_result:
            params, pv_power_simulated, error, valid = best_valid_result
            efficiency_size = params['efficiency_size']
            surface_azimuth = params['surface_azimuth']
            surface_tilt = params['surface_tilt']
            print(f"result_main:\tk={efficiency_size:.2%}\tazimuth={surface_azimuth}°\ttilt={surface_tilt}°\tpv_power_simulated={pv_power_simulated.mean():.2f}W\tfeedin_power_measured={profile['feedin'].mean():.2f}W\terror={error:.2f}W\tvalid={valid}")
        else:
            print(f'result_main:\tnone')

        # plot_results(results, profile, show_valid_results=True, show_invalid_results=True)

        if best_valid_result:
            params, _, _, _ = best_valid_result
            
            cls_pvmodel = SimplePVSystemModel(latitude, longitude, cls_irradiancemodel, **params)

            era5_irradiancemodel = ERA5IrradianceModel(latitude, longitude, era5data)
            era5_pvmodel = SimplePVSystemModel(latitude, longitude, era5_irradiancemodel, **params)

            compare_pv_models([cls_pvmodel, era5_pvmodel], profile)

        if best_valid_result and use_temperature_optimization:

            weathermodel = ERA5WeatherModel(era5data)

            pv_power_simulated_max = pv_power_simulated.groupby(pv_power_simulated.index.date).transform('max')
            closest_point_in_time = (pv_power_simulated_max - profile['feedin']).abs().idxmin()
            temp_baseline = weathermodel.simulate_temperature(closest_point_in_time)

            print('closest_point_in_time', closest_point_in_time)
            print('temp_baseline', temp_baseline)

            pvmodel_with_temp = SimplePVSystemModelWithTemperature(latitude, longitude, cls_irradiancemodel, weathermodel, **params, temp_baseline=temp_baseline)

            results, best_valid_result = fit_pv_parameters_temperature(pvmodel_with_temp, profile, verbose=True)

            plot_results(results, profile, show_valid_results=True, show_invalid_results=True)
            
            if best_valid_result:
                params, pv_power_simulated, error, valid = best_valid_result
                temp_coefficient = params['temp_coefficient']
                temp_baseline = params['temp_baseline']
                print(f"result_temp:\ttemp_baseline={temp_baseline:.2f}°C\tttemp_coefficient={temp_coefficient:.2%}\tpv_power_simulated={pv_power_simulated.mean():.2f}W\tfeedin_power_measured={profile['feedin'].mean():.2f}W\terror={error:.2f}W\tvalid={valid}")
            else:
                print(f'result_temp:\tnone')
    
        break
