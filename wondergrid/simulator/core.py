from pandas import DatetimeIndex

from .networkuser import NetworkUserModel


class Simulation:

  def __init__(self, networkusers: list[NetworkUserModel]):
    self.networkusers = networkusers
  
  def run(self, datetime_index: DatetimeIndex):
    for timestamp in datetime_index:
      for networkuser in self.networkusers:
        print(networkuser.id, networkuser.simulate(timestamp))
      break
