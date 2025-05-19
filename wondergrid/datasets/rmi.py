from __future__ import annotations

from typing import Type

import os
import time
import hashlib
import glob
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
import pyproj
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt

from pandas import DataFrame, DatetimeIndex, Timestamp, Period
from geopandas import GeoDataFrame
from shapely import Polygon, MultiPolygon, Point
from pyproj import CRS
from xarray import Dataset

DEFAULT_PROJECTED_CRS_BE = CRS.from_string('EPSG:3812')

from wondergrid.datasets.core import Dataset, DatasetBuilder, register_dataset_builder_cls


class RMIGriddedObservationsDatasetBuilder(DatasetBuilder):

  name = 'rmi/griddedobs'

  def __init__(self, start: Timestamp, end: Timestamp, proj_crs: CRS):
    super().__init__()
    self.start = start
    self.end = end
    self.proj_crs = CRS.from_user_input(proj_crs)

  def download_and_prepare(self):
    pass

  def load_as_dataset(self) -> RMIGriddedObservationsDataset:
    RMIGriddedObservationsDataset.load(self.start, self.end, self.proj_crs)


register_dataset_builder_cls(RMIGriddedObservationsDatasetBuilder)


class RMIGriddedObservationsDataset(Dataset):

  @classmethod
  def load(cls: Type[RMIGriddedObservationsDataset], start: Timestamp, end: Timestamp, proj_crs: CRS = DEFAULT_PROJECTED_CRS_BE) -> RMIGriddedObservationsDataset:

    builder = RMIGriddedObservationsDatasetBuilder(start, end, proj_crs)

    print(f'preparing {builder.name} dataset ...')

    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    proj_crs = CRS.from_user_input(proj_crs)

    identifier = hashlib.md5(str({ 'start': start, 'end': end }).encode()).hexdigest()

    outputfilepath = os.path.join(builder.get_data_path(), f'rmi-griddedobs-all-{identifier}.csv')

    if not os.path.exists(outputfilepath):

      dataframes = []

      for period in pd.period_range(start=start, end=end, freq='Y') :

        filepaths = glob.glob(os.path.join(builder.get_data_path(), f'*_{period.year}.csv'))
        
        if len(filepaths) == 0:
          raise ValueError(f'no observations found for period {period}')

        df_vars = []
        
        for filepath in filepaths:
          print(f'reading observations from {filepath} ...')
          df_var = pd.read_csv(filepath)
          df_var = df_var.rename(columns=lambda x: x.lower())
          df_var = df_var.drop(columns=['date'])
          df_var['timestamp'] = pd.to_datetime(df_var['timestamp'])
          df_var = df_var.set_index(['timestamp', 'latitude', 'longitude'])
          df_var = df_var.sort_index()
          df_vars.append(df_var)
        
        print(f'merging observations for period {period} ...')
        df_merged = pd.concat(df_vars, axis=1, verify_integrity=True)
        dataframes.append(df_merged)
      
      print(f'concatenating all observations ...')
      df_all = pd.concat(dataframes, axis=0, verify_integrity=True)
      df_all = df_all.sort_index()
      df_all = df_all.truncate(before=start, after=end)

      print(f'writing observations to {outputfilepath} ...')
      df_all.to_csv(outputfilepath)

    else:
      
      print(f'reading observations from {outputfilepath} ...')
      df_all = pd.read_csv(outputfilepath, index_col=['timestamp', 'latitude', 'longitude'], parse_dates=['timestamp'])

    print(f'loading as weather dataset ...')
    return cls(df_all, proj_crs)
  

  def __init__(self, data: DataFrame, proj_crs: CRS = DEFAULT_PROJECTED_CRS_BE):
    super().__init__()
    proj_crs = CRS.from_user_input(proj_crs)
    print(f'creating geodataframe from observations ...')
    self.data = data.unstack(level='timestamp').swaplevel(axis=1).sort_index(axis=1)
    points = gpd.points_from_xy(self.data.index.get_level_values('longitude'), self.data.index.get_level_values('latitude'), crs='EPSG:4326')
    gdf_points = gpd.GeoDataFrame(index=self.data.index, geometry=points).to_crs(proj_crs)
    gdf_tiles = gpd.GeoDataFrame(geometry=gdf_points.voronoi_polygons())
    gdf_tiles = gdf_tiles.sjoin(gdf_points).set_index(['latitude', 'longitude']).sort_index()
    self.points: GeoDataFrame = gdf_points
    self.tiles: GeoDataFrame = gdf_tiles

  @property
  def crs(self):
    return self.points.crs

  def get_weather_for_shape(self, shape: Polygon | MultiPolygon) -> DataFrame:
    areas = self.tiles.intersection(shape).area
    weather = self.data.mul(areas / areas.sum(), axis=0).sum(axis=0)
    return weather.unstack()
  
  def get_weather_for_location(self, location: Point) -> DataFrame:
    latlon = self.points.distance(location).idxmin()
    weather = self.data.loc[latlon]
    return weather.unstack()
  
  def __repr__(self):
    return self.data.__repr__()
  
  def __str__(self):
    return self.data.__str__()