from __future__ import annotations

import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt

from pandas import DataFrame, DatetimeIndex, Period
from geopandas import GeoDataFrame
from pyproj import CRS
from xarray import Dataset
from matplotlib.axes import Axes
from matplotlib.dates import DateFormatter

from wondergrid.datasets.core import Dataset


class SmartMeterDataset(Dataset):
  
  def __init__(self, profiles: Dataset, labels: DataFrame, locations: DataFrame):
    super().__init__()
    self.profiles = profiles
    self.labels = labels
    self.locations = locations

  def resample(self, frequency: str) -> SmartMeterDataset:
    profiles = self.profiles.resample(timestamp=frequency).mean()
    return SmartMeterDataset(profiles, self.labels, self.locations)
  
  def get_profile(self, id):
    profile = self.profiles.sel(id=id, drop=True).to_dataframe()
    label = self.labels.loc[id].to_dict()
    location = self.locations.loc[id].to_dict()
    return (id, profile, {**label, **location})
  
  def get_profiles(self):
    for id in self.profiles.id.values:
      yield self.get_profile(id)

  def get_mean_profile(self):
    return self.profiles.mean(dim='id').to_dataframe()

  def get_timeline(self) -> DatetimeIndex:
    return self.profiles['timestamp'].to_index()

  def get_locations_as_geodataframe(self, crs: CRS = None) -> GeoDataFrame:
    gdf_locations = gpd.GeoDataFrame(self.locations, geometry='shape')
    return gdf_locations.to_crs(CRS.from_user_input(crs)) if crs else gdf_locations

  @property
  def viewer(self):
    return SmartMeterDatasetViewer(self)


class SmartMeterDatasetViewer():
  
  def __init__(self, dataset: SmartMeterDataset):
    self.dataset = dataset
   
  def _update_plot(self, ax: Axes, id: int, period: Period):
    minvalue = self.dataset.profiles.min().to_array().min()
    maxvalue = self.dataset.profiles.max().to_array().max()
    id, profile, metadata = self.dataset.get_profile(id)
    profile = profile.loc[period.start_time:period.end_time]
    print(f"plot profile {id} on {period.start_time.date()}")
    ax.clear()
    profile.plot(ax=ax)
    ax.set_ylim(minvalue, maxvalue)
    ax.set_xlabel('Hour')
    ax.set_ylabel('Power')
    ax.set_title(f"Profile {id} on {period.start_time.date()}")


  def show_profile_interactive(self, id):
    assert id in self.dataset.profiles['id'].values
    timeline = self.dataset.get_timeline()
    periods = pd.period_range(start=timeline.min(), end=timeline.max(), freq='24h')

    fig, ax = plt.subplots()

    fig.subplots_adjust(bottom=0.25)

    ax_day = fig.add_axes([ax.get_position().x0, ax.get_position().y0 - 0.20, ax.get_position().width, 0.05])

    slides_day = mpl.widgets.Slider(ax_day, 'Day', 0, len(periods) - 1, valinit=0, valstep=1)

    slides_day.on_changed(lambda _: self._update_plot(ax, id, periods[slides_day.val]))

    self._update_plot(ax, id, periods[slides_day.val])

    fig.canvas.manager.set_window_title('Smart Meter Dataset Viewer')

    plt.show()
  

  def show_all_interactive(self):
    ids = self.dataset.profiles['id'].values
    timeline = self.dataset.get_timeline()
    periods = pd.period_range(start=timeline.min(), end=timeline.max(), freq='24h')

    fig, ax = plt.subplots()

    fig.subplots_adjust(bottom=0.30)

    ax_id = fig.add_axes([ax.get_position().x0, ax.get_position().y0 - 0.25, ax.get_position().width, 0.05])
    ax_day = fig.add_axes([ax.get_position().x0, ax.get_position().y0 - 0.20, ax.get_position().width, 0.05])

    slider_id = mpl.widgets.Slider(ax_id, 'Profile', 0, len(ids) - 1, valinit=0, valstep=1)
    slides_day = mpl.widgets.Slider(ax_day, 'Day', 0, len(periods) - 1, valinit=0, valstep=1)

    slider_id.on_changed(lambda _: self._update_plot(ax, ids[slider_id.val], periods[slides_day.val]))
    slides_day.on_changed(lambda _: self._update_plot(ax, ids[slider_id.val], periods[slides_day.val]))

    self._update_plot(ax, ids[slider_id.val], periods[slides_day.val])

    fig.canvas.manager.set_window_title('Smart Meter Dataset Viewer')

    plt.show()

  def show_overview(self):
    mean_profile = self.dataset.get_mean_profile()
    mean_profile['net_load'] = mean_profile['load'] - mean_profile['feedin']
    description='Load profile of all users'
    fig, axs = plt.subplots(3, layout='compressed')
    fig.suptitle(description, fontsize=16)
    im = axs[0].imshow(mean_profile['load'].to_numpy().reshape(365, 96).transpose(), norm=mpl.colors.CenteredNorm(), cmap='RdYlBu')
    axs[0].set_xlabel('Day of year')
    axs[0].set_ylabel('Quarter hour')
    axs[0].set_title('Load', fontsize=14)
    im = axs[1].imshow(mean_profile['feedin'].to_numpy().reshape(365, 96).transpose(), norm=mpl.colors.CenteredNorm(), cmap='RdYlBu')
    axs[1].set_xlabel('Day of year')
    axs[1].set_ylabel('Quarter hour')
    axs[1].set_title('Feedin', fontsize=14)
    im = axs[2].imshow(mean_profile['net_load'].to_numpy().reshape(365, 96).transpose(), norm=mpl.colors.CenteredNorm(), cmap='RdYlBu')
    axs[2].set_xlabel('Day of year')
    axs[2].set_ylabel('Quarter hour')
    axs[2].set_title('Net grid load', fontsize=14)
    clb = fig.colorbar(im, ax=axs, orientation='vertical')
    clb.set_label('Load (kW)')
    plt.show()