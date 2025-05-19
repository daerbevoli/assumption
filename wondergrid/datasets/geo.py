import os
import requests
import datetime
import urllib.parse

import pandas as pd
import geopandas as gpd
import pyproj

from pandas import Timestamp
from geopandas import GeoDataFrame
from pyproj import CRS

from wondergrid.datasets.core import Dataset, DatasetBuilder, register_dataset_builder_cls
from wondergrid.datasets.core import utils


DEFAULT_PROJECTED_CRS_BE = CRS.from_string('EPSG:3812')
DEFAULT_PROJECTED_CRS_US = CRS.from_string('ESRI:102003')



class GeoDataset(Dataset, GeoDataFrame):
  
  def __init__(self, data: GeoDataFrame):
    Dataset.__init__(self)
    GeoDataFrame.__init__(self, data)



class GeoBelgiumMunicipalitiesDatasetBuilder(DatasetBuilder):

  name = 'geo/be/municipalities'

  def __init__(self, reference_date: int | str | datetime.date | datetime.datetime):
    super().__init__()

    if isinstance(reference_date, int):
      reference_date = datetime.datetime.fromtimestamp(reference_date)
    elif isinstance(reference_date, str):
      reference_date = datetime.datetime.fromisoformat(reference_date)
    elif isinstance(reference_date, datetime.date):
      reference_date = datetime.datetime(reference_date.year, reference_date.month, reference_date.day)
     
    self.reference_date = reference_date


  def download_and_prepare(self):
    print(f'downloading and preparing {self.name} dataset ...')
    metadata_url = 'https://opendata.fin.belgium.be/download/ATOM/629ad470-71dc-11eb-af47-3448ed25ad7c-en.xml'
    print(f'fetching dataset metadata from {metadata_url}')
    response = requests.get(metadata_url)
    response.raise_for_status()
    
    reference_date = self.reference_date.strftime('%Y%m%d')
    data_url = f'https://opendata.fin.belgium.be/download/datasets/AU-RefSit_{reference_date}_shp_3812_01000.zip'
    data_file_path = os.path.join(self.get_data_path(), f'AU-RefSit_{reference_date}_shp_3812_01000.zip')
    data_file_path = utils.download(data_url, data_file_path)

    print(f'reading and parsing shapefile {data_file_path} ...')
    municipalities = gpd.read_file(data_file_path, layer='Apn_AdMu')  

    municipalities['name'] = municipalities.apply(lambda shape: shape['NameDUT'] if (shape['LangCode'][0] == 'D') else shape['NameFRE'] if shape['LangCode'][0] == 'F' else shape['NameGER'] if shape['LangCode'][0] == 'G' else '', axis=1).str.casefold()
    municipalities = municipalities.set_index('name')['geometry']

    output_data_file_path = self._get_data_file_path()
    municipalities.to_file(filename=output_data_file_path)
    print(f'saved shapefile to {output_data_file_path}')


  def load_as_dataset(self, crs: CRS = DEFAULT_PROJECTED_CRS_BE) -> GeoDataset:
    print(f'loading {self.name} as dataset ...')

    data_file_path = self._get_data_file_path()

    if not os.path.exists(data_file_path):
      raise FileNotFoundError()
    
    municipalities = gpd.read_file(data_file_path)
    municipalities = municipalities.rename(columns={'name': 'municipality'})
    municipalities = municipalities.set_index('municipality')
    municipalities = municipalities.to_crs(crs)

    return GeoDataset(municipalities)


  def _get_data_file_path(self):
    return os.path.join(self.get_data_path(), 'belgium-municipalities.shz')



class GeoBelgiumPostalDistrictsDatasetBuilder(DatasetBuilder):

  name = 'geo/be/postaldistricts'

  def __init__(self):
    super().__init__()


  def download_and_prepare(self):
    print(f'downloading and preparing {self.name} dataset ...')
    
    data_path = self.get_data_path()

    data_url = f'https://bgu.bpost.be/assets/9738c7c0-5255-11ea-8895-34e12d0f0423_x-shapefile_3812.zip'
    data_file_path = os.path.join(data_path, os.path.basename(urllib.parse.urlparse(data_url).path))
    data_file_path = utils.download(data_url, data_file_path)

    print(f'reading and parsing shapefile {data_file_path} ...')
    postaldistricts = gpd.read_file(f'{data_file_path}!3812')

    postaldistricts['postalcode'] = postaldistricts['nouveau_PO']
    postaldistricts = postaldistricts[['postalcode', 'geometry']]
    postaldistricts = postaldistricts.dissolve(by='postalcode')
    postaldistricts.geometry = postaldistricts.geometry.force_2d()

    output_data_file_path = self._get_data_file_path()
    postaldistricts.to_file(filename=output_data_file_path)
    print(f'saved shapefile to {output_data_file_path}')


  def load_as_dataset(self, crs: CRS = DEFAULT_PROJECTED_CRS_BE) -> GeoDataset:
    print(f'loading {self.name} as dataset ...')

    data_file_path = self._get_data_file_path()

    if not os.path.exists(data_file_path):
      raise FileNotFoundError()
    
    postaldistricts = gpd.read_file(data_file_path)
    postaldistricts = postaldistricts.set_index('postalcode')
    postaldistricts = postaldistricts.to_crs(crs)

    return GeoDataset(postaldistricts)


  def _get_data_file_path(self):
    return os.path.join(self.get_data_path(), 'belgium-postal-districts.shz')
  


register_dataset_builder_cls(GeoBelgiumMunicipalitiesDatasetBuilder)
register_dataset_builder_cls(GeoBelgiumPostalDistrictsDatasetBuilder)



class GeoUSDatasetBuilder(DatasetBuilder):

  name = 'geo/us/places'

  def __init__(self, year: int):
    super().__init__()
    self.year = year


  def download_and_prepare(self):
    print(f'downloading and preparing {self.name} dataset ...')
    data_path = self.get_data_path()

    state_codes_url = 'https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt'
    state_codes_file_path = os.path.join(data_path, 'national_state2020.txt')
    state_codes_file_path = utils.download(state_codes_url, state_codes_file_path)
    
    state_codes = pd.read_csv(state_codes_file_path, delimiter='|')

    shape_file_paths = []

    for statefp in state_codes['STATEFP']:
        shape_url = f'https://www2.census.gov/geo/tiger/TIGER{self.year:04d}/PLACE/tl_{self.year:04d}_{statefp:02d}_place.zip' 
        shape_file_path = os.path.join(data_path, f'tl_{self.year:04d}_{statefp:02d}_place.zip')
        shape_file_path = utils.download(shape_url, shape_file_path)
        if shape_file_path:
          shape_file_paths.append(shape_file_path)

    print(f'transforming shapefiles ...')

    data_file_path = self._get_data_file_path()

    if os.path.exists(data_file_path):
      print(f'skipped shapefile transform, file {data_file_path} already exists')
    else:
      shapefiles = []
      for shapefilepath in shape_file_paths:
        print(f'reading and parsing shapefile {shapefilepath} ...')
        shapefiles.append(gpd.read_file(shapefilepath))

      places = pd.concat(shapefiles, ignore_index=True)

      places['STATEFP'] = pd.to_numeric(places['STATEFP'])
      places['PLACEFP'] = pd.to_numeric(places['PLACEFP'])
      places['PLACENS'] = pd.to_numeric(places['PLACENS'])
      places['GEOID'] = pd.to_numeric(places['GEOID'])

      places = pd.merge(places, state_codes, on=['STATEFP'], validate='many_to_one')

      places = gpd.GeoDataFrame(places).to_crs('ESRI:102003')
      
      places.to_file(data_file_path)
      print(f'saved shapefile to {data_file_path}')


  def load_as_dataset(self, crs: CRS = DEFAULT_PROJECTED_CRS_US) -> GeoDataset:
    print(f'loading {self.name} as dataset ...')

    data_file_path = self._get_data_file_path()

    if not os.path.exists(data_file_path):
      raise FileNotFoundError()
    
    usplaces = gpd.read_file(data_file_path)
    usplaces = usplaces.to_crs(crs)

    return GeoDataset(usplaces)


  def _get_data_file_path(self):
    return os.path.join(self.get_data_path(), 'unitedstates-places.shz')
  


register_dataset_builder_cls(GeoUSDatasetBuilder)