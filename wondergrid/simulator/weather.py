from pandas import DataFrame, Timestamp
from shapely import Polygon, MultiPolygon, Point


class WeatherModel:
  
  def __init__(self):
    # simulation state
    self._state = {}

  @property
  def state(self) -> dict[str, float]:
    return self._state

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    pass


class DummyWeatherModel(WeatherModel):

  def __init__(self):
    super().__init__()

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # simulation state
    self._state = { "temperature": 25, "precipitation": 0 }
    return self._state


class DataReplayWeatherModel(WeatherModel):

  def __init__(self, weather: DataFrame):
    super().__init__()
    # properties
    self.weather = weather

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # simulation state
    self._state = self.weather.loc[timestamp].to_dict()
    return self._state
