import dotenv

import numpy as np
import pandas as pd

dotenv.load_dotenv()

from wondergrid.datasets import load_dataset
from wondergrid.datasets.geo import GeoDataset
from wondergrid.datasets.dmk import DMKDataset
from wondergrid.datasets.era5 import ERA5Dataset
from wondergrid.simulator.weather import DataReplayWeatherModel
from wondergrid.simulator.networkuser import DataReplayNetworkUserModel
from wondergrid.simulator.core import Simulation


def run():
    dmkdataset: DMKDataset = load_dataset('fluvius/dmk')

    # municipalities: GeoDataset = load_dataset('geo/be/municipalities', reference_date='2022-01-01')
    # dmkdataset = dmkdataset.add_municipalities(municipalities)
    postaldistricts: GeoDataset = load_dataset('geo/be/postaldistricts')
    dmkdataset = dmkdataset.add_postal_districts(postaldistricts)

    dmkdataset = dmkdataset.filter(n=1)

    timeline = dmkdataset.get_timeline()

    # bounds = dmkdataset.get_locations_as_geodataframe('EPSG:4326').total_bounds
    # bounds = municipalities.to_crs('EPSG:4326').total_bounds
    bounds = postaldistricts.to_crs('EPSG:4326').total_bounds

    networkusers = []

    for (iid, profile, metadata) in dmkdataset.get_profiles():
        profile = profile.resample('h').mean()

        print(profile)
        #print(metadata)

        networkuser = DataReplayNetworkUserModel(iid, profile, metadata)
        networkusers.append(networkuser)

    simulation = Simulation(networkusers)

    timeline = pd.date_range(start='2022', end='2023', freq='h', inclusive='left')

    simulation.run(timeline)


if __name__ == "__main__":
    run()