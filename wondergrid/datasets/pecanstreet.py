from __future__ import annotations

import os
import urllib.parse
import tarfile
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import humanize
import matplotlib as mpl
import matplotlib.pyplot as plt

from pandas import DataFrame
from geopandas import GeoDataFrame
from xarray import Dataset

from wondergrid.datasets.core import Dataset, DatasetBuilder, register_dataset_builder_cls
from wondergrid.datasets.core import utils
from wondergrid.datasets.smartmeter import SmartMeterDataset



class PecanStreetDataset(SmartMeterDataset):
  
  def __init__(self, profiles: Dataset, labels: DataFrame, locations: DataFrame):
    super().__init__(profiles, labels, locations)

  def add_places(self, places: GeoDataFrame) -> PecanStreetDataset:
    places = places[['STATE_NAME', 'NAME', 'geometry']].set_index(['STATE_NAME', 'NAME'])
    places = places.rename_geometry('shape')
    places['location'] = places.centroid
    coordinates = places['location'].to_crs('EPSG:4326')
    places['longitude'] = coordinates.x
    places['latitude'] = coordinates.y
    places['minimum_bounding_radius'] = places.minimum_bounding_radius()
    locations = self.locations.join(places, on=['state', 'city'])
    return PecanStreetDataset(self.profiles, self.labels, locations)

  def filter(self, pv: bool = None, max_radius: float = None) -> PecanStreetDataset:
    mask_pv = ((pv == None) | (self.labels['pv'] == pv))
    mask_mbr = ((max_radius == None) | (self.locations['minimum_bounding_radius'] < max_radius))
    mask = (self.labels['grid'] & mask_pv & mask_mbr).to_numpy()
    profiles = self.profiles.loc[{'id': mask}]
    labels = self.labels.loc[mask]
    locations = self.locations.loc[mask]
    profiles = profiles.dropna(dim='timestamp', how='all')
    return PecanStreetDataset(profiles, labels, locations)
  
  def head(self, n: int) -> PecanStreetDataset:
    profiles = self.profiles.head({'id': n})
    labels = self.labels.head(n)
    locations = self.locations.head(n)
    profiles = profiles.dropna(dim='timestamp', how='all')
    return PecanStreetDataset(profiles, labels, locations)



class PecanStreetDatasetBuilder(DatasetBuilder):

  name = 'pecanstreet'

  def __init__(self):
    super().__init__()

  def download_and_prepare(self):
    print(f'downloading and preparing {self.name} dataset ...')

    data_path = self.get_data_path()

    extracted_file_paths = []
    extracted_dir_paths = []

    for data_url in self._build_download_urls():
        data_file_name = os.path.basename(urllib.parse.urlparse(data_url).path)
        data_file_path = os.path.join(data_path, data_file_name)
        data_file_path = utils.download(data_url, data_file_path)
        if data_file_path:
          print(f'extracting {data_file_path} ...')
          with tarfile.open(data_file_path) as compressed_data_file:
            for member in compressed_data_file.getmembers():
              if member.isfile():
                extracted_file_path = os.path.join(data_path, member.name)
                new_extracted_file_path = os.path.join(data_path, os.path.basename(member.name))
                if os.path.exists(new_extracted_file_path):
                  print(f'skipped extraction, file {new_extracted_file_path} already exists')
                else:
                  compressed_data_file.extract(member.name, data_path)
                  os.replace(extracted_file_path, new_extracted_file_path)
                  print(f'extracted file {new_extracted_file_path} ({humanize.naturalsize(os.path.getsize(new_extracted_file_path))})')
                extracted_file_paths.append(new_extracted_file_path)
              if member.isdir():
                extracted_dir_paths.append(os.path.join(data_path, member.name))

    print(f'cleaning up after extraction ...')
    for extracted_dir_path in extracted_dir_paths:
      if os.path.exists(extracted_dir_path):
        os.rmdir(extracted_dir_path)
        print(f'removed directory {extracted_dir_path}')
    
    print(f'transforming profiles, labels and locations ...')

    profiles_path, labels_path, locations_path = self._get_data_file_paths()

    if not (os.path.exists(profiles_path) and os.path.exists(labels_path) and os.path.exists(locations_path)):
      data_file_path_austin = os.path.join(data_path, '15minute_data_austin.csv')
      print(f'reading and parsing csv file {data_file_path_austin} ...')
      df_pecanstreet_austin = pd.read_csv(data_file_path_austin, usecols=['dataid', 'local_15min', 'grid', 'solar', 'solar2'])
      data_file_path_california = os.path.join(data_path, '15minute_data_california.csv')
      print(f'reading and parsing csv file {data_file_path_california} ...')
      df_pecanstreet_california = pd.read_csv(data_file_path_california, usecols=['dataid', 'local_15min', 'grid', 'solar', 'solar2'])
      data_file_path_newyork = os.path.join(data_path, '15minute_data_newyork.csv')
      print(f'reading and parsing csv file {data_file_path_newyork} ...')
      df_pecanstreet_newyork = pd.read_csv(data_file_path_newyork, usecols=['dataid', 'local_15min', 'grid', 'solar', 'solar2'])
      df_pecanstreet = pd.concat([df_pecanstreet_austin, df_pecanstreet_california, df_pecanstreet_newyork])

    if os.path.exists(profiles_path):
      print(f'skipped profile transform, file {profiles_path} already exists')
    else:
      df_pecanstreet['local_15min'] = pd.to_datetime(df_pecanstreet['local_15min'], utc=True).dt.tz_localize(None)
      df_pecanstreet[['grid', 'solar', 'solar2']] = df_pecanstreet[['grid', 'solar', 'solar2']].fillna(0) * 1000    # transform kW to W
      df_pecanstreet['load'] = df_pecanstreet['grid'].where(df_pecanstreet['grid'] > 0, 0)
      df_pecanstreet['feedin'] = - df_pecanstreet['grid'].where(df_pecanstreet['grid'] < 0, 0)
      df_pecanstreet['consumption'] = df_pecanstreet['grid'] + df_pecanstreet['solar'] + df_pecanstreet['solar2']
      df_pecanstreet['production'] = df_pecanstreet['solar'] + df_pecanstreet['solar2']
      df_pecanstreet = df_pecanstreet.drop(columns=['grid', 'solar', 'solar2'])
      xr_pecanstreet = df_pecanstreet.rename(columns={'dataid': 'id', 'local_15min': 'timestamp'}).set_index(['id', 'timestamp']).to_xarray()
      xr_pecanstreet.to_netcdf(profiles_path, engine='netcdf4')
      print(f'saved profiles to netcdf4 file {profiles_path}')

    if os.path.exists(labels_path):
      print(f'skipped label transform, file {labels_path} already exists')
    else:
      data_file_path_metadata = os.path.join(data_path, 'metadata.csv')
      print(f'reading and parsing csv file {data_file_path_metadata} ...')
      df_pecanstreet_labels = pd.read_csv(data_file_path_metadata, skiprows=[1], usecols=['dataid', 'grid', 'pv', 'pv_panel_direction', 'total_amount_of_pv', 'amount_of_south_facing_pv', 'amount_of_west_facing_pv', 'amount_of_east_facing_pv'], index_col=['dataid'])    
      df_pecanstreet_labels = df_pecanstreet_labels.loc[df_pecanstreet['dataid'].unique()]
      df_pecanstreet_labels = df_pecanstreet_labels[['grid', 'pv', 'pv_panel_direction', 'total_amount_of_pv', 'amount_of_south_facing_pv', 'amount_of_west_facing_pv', 'amount_of_east_facing_pv']]
      df_pecanstreet_labels['grid'] = (df_pecanstreet_labels['grid'] == 'yes').astype(int)
      df_pecanstreet_labels['pv'] = (df_pecanstreet_labels['pv'] == 'yes').astype(int)
      df_pecanstreet_labels['pv_panel_direction'] = df_pecanstreet_labels['pv_panel_direction'].str.upper()
      df_pecanstreet_labels = df_pecanstreet_labels.rename_axis(index={'dataid': 'id'})
      df_pecanstreet_labels.to_csv(labels_path)
      print(f'saved labels to csv file {labels_path}')
    
    if os.path.exists(locations_path):
      print(f'skipped location transform, file {locations_path} already exists')
    else:
      df_pecanstreet_locations = pd.read_csv(os.path.join(data_path, 'metadata.csv'), skiprows=[1], usecols=['dataid', 'city', 'state'], index_col=['dataid'])    
      df_pecanstreet_locations = df_pecanstreet_locations.loc[df_pecanstreet['dataid'].unique()]
      df_pecanstreet_locations = df_pecanstreet_locations.rename_axis(index={'dataid': 'id'})
      df_pecanstreet_locations.to_csv(locations_path)
      print(f'saved locations to csv file {locations_path}')
  
    
  def load_as_dataset(self) -> PecanStreetDataset:
    print(f'loading {self.name} as dataset ...')

    data_file_paths = self._get_data_file_paths()
    
    for data_file_path in data_file_paths:
      if not os.path.exists(data_file_path):
        raise FileNotFoundError()
    
    profiles_path, labels_path, locations_path = data_file_paths

    profiles = xr.open_dataset(profiles_path)
    labels = pd.read_csv(labels_path, index_col='id').sort_index()
    locations = pd.read_csv(locations_path, index_col='id').sort_index()

    return PecanStreetDataset(profiles, labels, locations)


  def _build_download_urls(self):
    yield 'https://dataport.pecanstreet.org/static/static_files/New_York/15minute_data_newyork.tar.gz'
    yield 'https://dataport.pecanstreet.org/static/static_files/California/15minute_data_california.tar.gz'
    yield 'https://dataport.pecanstreet.org/static/static_files/Austin/15minute_data_austin.tar.gz'
    # yield 'https://dataport.pecanstreet.org/static/metadata.csv'

  def _get_data_file_paths(self):
    data_path = self.get_data_path()
    profiles_path = os.path.join(data_path, 'pecanstreet_sample_75.nc')
    labels_path = os.path.join(data_path, 'pecanstreet_sample_75_labels.csv')
    locations_path = os.path.join(data_path, 'pecanstreet_sample_75_locations.csv')
    return (profiles_path, labels_path, locations_path)


register_dataset_builder_cls(PecanStreetDatasetBuilder)