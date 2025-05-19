import random
import pandas as pd
import pvlib


from pandas import DataFrame, DatetimeIndex, Timestamp


class Asset():

  def __init__(self) -> None:
    # simulation state
    self._state = {}
    self._history = None

  @property
  def state(self) -> dict[str, float]:
    return self._state

  @property
  def consumption(self) -> float:
    return self._state.get('consumption', 0)
  
  @property
  def production(self) -> float:
    return self._state.get('production', 0)

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # use asset properties, settings, state and weather, building, and residents info to simulate power consumption
    return self.state


class PhotoVoltaicAsset(Asset):
  
  def __init__(self) -> None:
    super().__init__()

  def generate(self, timeline: DatetimeIndex):
    pass

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    pass

class ClearSkyPhotoVoltaicAsset(PhotoVoltaicAsset):
  
  def __init__(self, latitude: float, longitude: float, surface_tilt: float = 43.0, surface_azimuth: float = 180.0, efficiency_size: float = 2.0) -> None:
    super().__init__()
    # asset properties
    self.location = pvlib.location.Location(latitude, longitude)
    self._params = {
      'surface_tilt': surface_tilt,
      'surface_azimuth': surface_azimuth,
      'efficiency_size': efficiency_size
    }

  @property
  def surface_tilt(self) -> float:
    return self._params.get('surface_tilt')
  
  @surface_tilt.setter
  def surface_tilt(self, value: float) -> None:
    self._params['surface_tilt'] = value
  
  @property
  def surface_azimuth(self) -> float:
    return self._params.get('surface_azimuth')
  
  @surface_azimuth.setter
  def surface_azimuth(self, value: float) -> None:
    self._params['surface_azimuth'] = value

  @property
  def efficiency_size(self) -> float:
    return self._params.get('efficiency_size')
  
  @efficiency_size.setter
  def efficiency_size(self, value: float) -> None:
    self._params['efficiency_size'] = value

  def generate(self, timeline: DatetimeIndex) -> DataFrame:
    solarposition = self.location.get_solarposition(timeline)
    irradiance = self.location.get_clearsky(timeline)
    poa_irradiance = pvlib.irradiance.get_total_irradiance(
      surface_tilt=self.surface_tilt,
      surface_azimuth=self.surface_azimuth,
      solar_zenith=solarposition['apparent_zenith'],
      solar_azimuth =solarposition['azimuth'],
      dni=irradiance['dni'],
      ghi=irradiance['ghi'],
      dhi=irradiance['dhi'],
      model='isotropic'
    )
    # temp_coefficient = (1 + params['temp_coefficient']) * (params['temp_baseline'] - weather['t2m'])
    pv_power = poa_irradiance['poa_global'] * self.efficiency_size
    self._history = pd.concat([solarposition, irradiance, poa_irradiance, pv_power.rename('pv_power')], axis=1)
    return self._history

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    if self._history and timestamp in self._history.index:
      return self._history.loc[timestamp]
    else:
      return self.generate(pd.DatetimeIndex([timestamp]))


class BaseLoadAsset(Asset):
  
  def __init__(self, meanpower, stdvpower) -> None:
    super().__init__()
    # asset properties
    self._meanpower = meanpower
    self._stdvpower = stdvpower

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # use asset properties, settings, state and weather, building, and residents info to simulate power consumption
    self._consumption = max(random.gauss(self._meanpower, self._stdvpower), 0)
    # self._consumption = max(self._meanpower + (1 + math.sin(timestamp.time)) / 2 * self._stdvpower, 0)
    return self.state


class SteerableAsset(Asset):

  def __init__(self, maxconsumption: float, steerableconsumption: bool, maxproduction: float, steerableproduction: bool) -> None:
    super().__init__()
    # asset properties
    self._maxconsumption = maxconsumption
    self._steerableconsumption = steerableconsumption
    self._maxproduction = maxproduction
    self._steerableproduction = steerableproduction
    # asset settings
    self._requestedconsumption = 0
    self._requestedproduction = 0

  @property
  def maxconsumption(self) -> float:
    return self._maxconsumption
  
  @property
  def steerableconsumption(self) -> bool:
    return self._steerableconsumption

  @property
  def maxproduction(self) -> float:
    return self._maxproduction
  
  @property
  def steerableproduction(self) -> bool:
    return self._steerableproduction
  
  @property
  def requestedconsumption(self) -> float:
    return self._requestedconsumption
  
  @requestedconsumption.setter
  def requestedconsumption(self, power: float) -> None:
    assert self.steerableconsumption, "asset's power consumption is not steerable"
    assert power >= 0, "requested power consumption must be a positive value"
    assert power <= self._maxconsumption, "requested power consumption must be lower than max power consumption"
    self._requestedconsumption = power
    self._requestedproduction = 0
  
  @property
  def requestedproduction(self) -> float:
    return self._requestedproduction
  
  @requestedproduction.setter
  def requestedproduction(self, power: float) -> None:
    assert self.steerableproduction, "asset's power production is not steerable"
    assert power >= 0, "requested power production must be a positive value"
    assert power <= self._maxproduction, "requested power production must be lower than max power production"
    self._requestedproduction = power
    self._requestedconsumption = 0



class HeatPumpAsset(SteerableAsset):
  
  def __init__(self, maxheatingpower: float, efficiency: float) -> None:
    super().__init__(maxheatingpower, False, 0, True)
    # asset properties
    self._maxheatingpower = maxheatingpower
    self._efficiency = efficiency
    # simulation state
    self._internaltemperature = 0

  @property
  def maxheatingpower(self) -> float:
    return self._maxheatingpower
  
  @property
  def efficiency(self) -> float:
    return self._efficiency
  
  @property
  def indoortemperature(self) -> float:
    return self._internaltemperature
  
  @property
  def state(self):
    state = super().state
    state["internaltemperature"] = self._internaltemperature
    return state

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # use asset properties, settings, state and weather, building, and residents info to simulate power consumption
    self._consumption = self._requestedconsumption
    return self.state



class BatteryAsset(SteerableAsset):
  
  def __init__(self, capacity: float, maxchargingpower: float, maxdischargingpower: float, chargingefficiency: float = 1.0, dischargingefficiency: float = 1.0) -> None:
    super().__init__(maxchargingpower, True, maxdischargingpower, True)
    # asset properties
    self._capacity = capacity
    self._maxchargingpower = maxchargingpower
    self._chargingefficiency = chargingefficiency
    self._maxdischargingpower = maxdischargingpower
    self._dischargingefficiency = dischargingefficiency
    # simulation state
    self._stateofcharge = 0

  @property
  def capacity(self) -> float:
    return self._capacity

  @property
  def maxchargingpower(self) -> float:
    return self._maxchargingpower
  
  @property
  def chargingefficiency(self) -> float:
    return self._chargingefficiency

  @property
  def maxdischargingpower(self) -> float:
    return self._maxdischargingpower
  
  @property
  def dischargingefficiency(self) -> float:
    return self._dischargingefficiency

  @property
  def state(self):
    state = super().state
    state["stateofcharge"] = self._stateofcharge
    return state
  
  def updatestateofcharge(self, timestamp) -> None:
    charge = self._consumption * self._chargingefficiency * timestamp["duration"]
    discharge = self._production * self._dischargingefficiency * timestamp["duration"]
    self._stateofcharge = max(min(self._stateofcharge + charge - discharge, self._capacity), 0)

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # use asset properties, settings, state and weather, building, and residents info to simulate power consumption
    self.updatestateofcharge(timestamp)
    self._consumption = self._requestedconsumption
    self._production = self._requestedproduction
    return self.state



class HomeBatteryAsset(BatteryAsset):
  pass



class ElectricVehicleAsset(BatteryAsset):
  
  def __init__(self, capacity: float, maxchargingpower: float, maxdischargingpower: float, chargingefficiency: float = 1.0, dischargingefficiency: float = 1.0) -> None:
    super().__init__(capacity, maxchargingpower, maxdischargingpower, chargingefficiency, dischargingefficiency)
    # asset settings
    self._isconnected = False

  @property
  def isconnected(self) -> bool:
    return self._isconnected
  
  @isconnected.setter
  def isconnected(self, value: bool) -> None:
    self._isconnected = value
    self._requestedconsumption = self._requestedconsumption if value else 0
    self._requestedproduction = self._requestedproduction if value else 0

  @property
  def requestedconsumption(self) -> float:
    super().requestedconsumption
  
  @requestedconsumption.setter
  def requestedconsumption(self, power: float) -> None:
    assert self.isconnected, "electric vehicle must be connected to steer power consumption"
    super().requestedconsumption = power
  
  @property
  def requestedproduction(self) -> float:
    super().requestedproduction
  
  @requestedproduction.setter
  def requestedproduction(self, power: float) -> None:
    assert self.isconnected, "electric vehicle must be connected to steer power production"
    super().requestedproduction = power
  
  @property
  def state(self):
    state = super().state
    state["isconnected"] = self._isconnected
    return state