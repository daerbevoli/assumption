from pandas import DataFrame, DatetimeIndex, Timestamp

from .weather import WeatherModel

class NetworkUserModel:

  def __init__(self, id: str, latitude: float, longitude: float):
    # properties
    self.id = id
    self.latitude = latitude
    self.longitude = longitude
    # simulation state
    self._state = {}

  @property
  def state(self) -> dict[str, float]:
    return self._state

  @property
  def load(self) -> float:
    return self._state.get('load', 0)
  
  @property
  def feedin(self) -> float:
    return self._state.get('feedin', 0)

  def generate(self, timeline: DatetimeIndex) -> DataFrame:
    pass

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    pass
  

class DataReplayNetworkUserModel(NetworkUserModel):

  def __init__(self, id: str, profile: DataFrame, metadata: dict):
    super().__init__(id, metadata['latitude'], metadata['longitude'])
    # properties
    self.profile = profile
    self.metadata = metadata

  def generate(self, timeline: DatetimeIndex) -> DataFrame:
    self._history = self.profile.loc[timeline]
    return self._history

  def simulate(self, timestamp: Timestamp) -> dict[str, float]:
    # simulation state
    self._state = self.profile.loc[timestamp].to_dict()
    return self._state
