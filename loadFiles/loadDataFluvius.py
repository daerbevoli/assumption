import dotenv

import pandas as pd

dotenv.load_dotenv()

from wondergrid.datasets import load_dataset
from wondergrid.datasets.dmk import DMKDataset


def loadFluviusData(amountToFilter: int):
    # Initialize the dataset for residential units
    dmkdataset: DMKDataset = load_dataset('fluvius/dmk')

    # We will work with 5 sets for now
    dmkdataset = dmkdataset.filter(n=amountToFilter).resample('h')

    profile = pd.DataFrame

    # Make a list of sets to randomly choose from
    meters = []
    # load the agent with the profile
    for (iid, profile, metadata) in dmkdataset.get_profiles():
        profile[['load', 'feedin']] = profile[['load', 'feedin']] * 1e-3  # Convert kWh to MWh
        meters.append(profile)

    return meters