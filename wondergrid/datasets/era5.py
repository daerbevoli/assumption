import os
import hashlib
import cdsapi
import zipfile
import numpy as np
import pandas as pd
import geopandas as gpd
import pyproj
import xarray as xr
import humanize

from typing import Sequence
from numpy.typing import NDArray
from pandas import Timestamp, DataFrame
from geopandas import GeoDataFrame
from shapely import Polygon, MultiPolygon, Point
from pyproj import CRS
from xarray import Dataset

DEFAULT_PROJECTED_CRS = CRS.from_string('EPSG:3857')

from wondergrid.datasets.core import Dataset, DatasetBuilder, register_dataset_builder_cls



class ERA5Dataset(Dataset):
  
  def __init__(self, data, points, tiles, proj_crs: CRS):
    super().__init__()
    self.data = data
    self.points = points
    self.tiles = tiles
    self.proj_crs = CRS.from_user_input(proj_crs)

  def get_weather_for_shape(self, shape: Polygon | MultiPolygon) -> DataFrame:
    areas = self.tiles.intersection(shape).area
    areas = areas / areas.sum()
    weights = xr.DataArray.from_series(areas)
    weather = self.data.weighted(weights.fillna(0)).sum(dim=('latitude', 'longitude'))
    return weather.to_dataframe()
  
  def get_weather_for_location(self, location: Point) -> DataFrame:
    selection = self.tiles.contains(location)
    lat, lon = self.tiles[selection].index[0]
    weather = self.data.sel(latitude=lat, longitude=lon, drop=True)
    return weather.to_dataframe()



class ERA5DatasetBuilder(DatasetBuilder):

  name = 'ecmwf/era5'

  def __init__(self, bounds: Sequence | NDArray, start_date: Timestamp, end_date: Timestamp, proj_crs: CRS = DEFAULT_PROJECTED_CRS, cds_api_url: str = None, cds_api_key: str = None):
    super().__init__()
    self._cds_api_url = cds_api_url if cds_api_url else os.environ.get('CDSAPI_URL')
    self._cds_api_key = cds_api_key if cds_api_key else os.environ.get('CDSAPI_KEY')
    self.bounds = tuple(map(float, bounds))
    self.start_date = start_date
    self.end_date = end_date
    self.proj_crs = CRS.from_user_input(proj_crs)


  def download_and_prepare(self):
    print(f'downloading and preparing {self.name} dataset ...')

    data_path = self.get_data_path()

    for resource, request, target in self._build_cds_api_requests():

      compressed_data_file_path = f'{target}.zip'
    
      if os.path.exists(compressed_data_file_path):
        print(f'skipped download, file {compressed_data_file_path} already exists')
      else:
        print(f'initializing cds api client {self._cds_api_url} ...')
        cdsclient = cdsapi.Client(url=self._cds_api_url, key=self._cds_api_key)
        print(f'submitting cds api request {request} ...')
        result = cdsclient.retrieve(resource, request)
        print(f'downloading cds api result {result.info} ...')
        compressed_data_file_path = result.download(compressed_data_file_path)
        print(f'saved dowloaded file to {compressed_data_file_path} ({humanize.naturalsize(os.path.getsize(compressed_data_file_path))})')
      
      data_file_path = target

      if compressed_data_file_path:

        print(f'extracting {compressed_data_file_path} ...')

        if os.path.exists(data_file_path):
          print(f'skipped extraction, file {data_file_path} already exists')
        else:
      
          extracted_file_paths = []
        
          with zipfile.ZipFile(compressed_data_file_path) as compressed_data_file:
            for zipinfo in compressed_data_file.infolist():
              extracted_file_path = compressed_data_file.extract(zipinfo, data_path)
              print(f'extracted file {extracted_file_path} ({humanize.naturalsize(os.path.getsize(extracted_file_path))})')
              extracted_file_paths.append(extracted_file_path)

          print(f'combining extracted netcdf files ...')
          combined_dataset = xr.open_mfdataset(extracted_file_paths)
          combined_dataset = combined_dataset.rename({ 'valid_time': 'timestamp' })
          combined_dataset.to_netcdf(target, engine='netcdf4')
          print(f'saved combined netcdf file to {target}')

          print(f'cleaning up after extraction ...')

          for extracted_file_path in extracted_file_paths:
            if os.path.exists(extracted_file_path):
              os.remove(extracted_file_path)
              print(f'removed file {extracted_file_path}')

    
  def load_as_dataset(self) -> ERA5Dataset:
    print(f'loading {self.name} as dataset ...')

    data_file_paths = self._get_data_file_paths()
    
    for data_file_path in data_file_paths:
      if not os.path.exists(data_file_path):
        raise FileNotFoundError()
    
    data = xr.open_mfdataset(data_file_paths, drop_variables=['number', 'expver'])

    longitudes, latitudes = np.meshgrid(data.longitude, data.latitude)
    x, y = pyproj.Transformer.from_crs('EPSG:4326', self.proj_crs).transform(latitudes, longitudes)
    data = data.assign_coords(x=(('latitude', 'longitude'), x), y=(('latitude', 'longitude'), y))
    data = data.assign_attrs(crs=self.proj_crs)
    coordinates = data[['x', 'y']].to_dataframe()
    gdf_points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(coordinates['x'], coordinates['y']), crs=self.proj_crs)
    gdf_tiles = gpd.GeoDataFrame(geometry=gdf_points.voronoi_polygons())
    gdf_tiles = gdf_tiles.sjoin(gdf_points).set_index('index_right').sort_index().reset_index().drop('index_right', axis=1)
    points : GeoDataFrame = gdf_points.set_index(coordinates.index)
    tiles : GeoDataFrame = gdf_tiles.set_index(coordinates.index)
    return ERA5Dataset(data, points, tiles, self.proj_crs)


  def _build_cds_api_requests(self):

    # for period in pd.period_range(self.start_date, self.end_date, freq='M'):

    data_path = self.get_data_path()

    resource = 'reanalysis-era5-single-levels'

    # timeline = pd.date_range(self.start_date, self.end_date, freq='h')

    request = {
      'product_type': ['reanalysis'],
      'variable': [
        '2m_temperature',
        'surface_pressure',
        '10m_u_component_of_wind',
        '10m_v_component_of_wind',
        'total_cloud_cover',
        'total_precipitation',
        'surface_solar_radiation_downwards',
        'total_sky_direct_solar_radiation_at_surface'
      ],
      'date': f'{self.start_date.date()}/{self.end_date.date()}',
      'time': [h for h in range(24)],
      # 'year': timeline.year.unique().sort_values().to_list(),
      # 'month': timeline.month.unique().sort_values().to_list(),
      # 'day': timeline.day.unique().sort_values().to_list(),
      # 'time': timeline.hour.unique().sort_values().to_list(),
      'area': self.bounds,
      'data_format': 'netcdf',
      'download_format': 'unarchived',
    }

    identifier = hashlib.md5(str(request).encode()).hexdigest()

    target = os.path.join(data_path, f'{resource}-{identifier}.nc')

    yield resource, request, target


  def _get_data_file_paths(self):
    return [target for resource, request, target in self._build_cds_api_requests()]


register_dataset_builder_cls(ERA5DatasetBuilder)
