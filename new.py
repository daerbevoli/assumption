import logging
from datetime import datetime, timedelta

import pandas as pd
from dateutil import rrule as rr

from assume import World
from assume.common.forecasts import NaiveForecast
from assume.common.market_objects import MarketConfig, MarketProduct

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



from ResidentAgent import ResidentAgent

log = logging.getLogger(__name__)

db_uri = "sqlite:///local_db/assume_db.db"

world = World(database_uri=db_uri)

start = datetime(2022, 1, 1)
end = datetime(2022, 12, 31)
index = pd.date_range(
    start=start,
    end=end + timedelta(hours=24),
    freq="h",
)
sim_id = "world_script_simulation"

world.setup(
    start=start,
    end=end,
    # save_frequency_hours=48,
    save_frequency_hours=24,
    simulation_id=sim_id,
    #index=index,
)


# Configuration of the energy market
# Only one market
marketConf = MarketConfig(
        market_id="EOM", # Energy Only Market
        opening_hours=rr.rrule(rr.HOURLY, interval=24, dtstart=start, until=end),
        opening_duration=timedelta(hours=1),
        market_mechanism="pay_as_clear",
        market_products=[MarketProduct(timedelta(hours=1), 24, timedelta(hours=1))],
        maximum_bid_volume=20000,
        maximum_bid_price=100,
        additional_fields=["block_id", "link", "exclusive_id"],
    )


mo_id = "market_operator"
world.add_market_operator(id=mo_id)

world.add_market(market_operator_id=mo_id, market_config=marketConf)

# Initialize the dataset
dmkdataset: DMKDataset = load_dataset('fluvius/dmk')
dmkdataset = dmkdataset.filter(n=1)

# Initialize the agent
residence = ResidentAgent()

# load the agent with the first profile (draft)
for profile in dmkdataset.get_profiles():
    profile = profile.resample('h').mean()
    residence.set_load(profile)

residenceAgent = {
    "id": "agent1",
    "data": residence.get_load(),
    "role": "consumer"

}

# Get panda Series of timestamp and load
agentDemand = residence.get_load()['load']

print(agentDemand)

world.add_unit_operator("demand_operator")


# Link demand list with forecaster (failed)
demand_forecast = NaiveForecast(index, demand=agentDemand)

world.add_unit(
    id="agent1",
    unit_type="demand",
    unit_operator_id="demand_operator",
    unit_params={
        "min_power": 0,
        "max_power": 4000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "demand",
    },
    forecaster=demand_forecast,
)

world.add_unit_operator("unit_operator")

nuclear_forecast = NaiveForecast(index, availability=1, fuel_price=3, co2_price=0.1)

world.add_unit(
    id="nuclear_unit",
    unit_type="power_plant",
    unit_operator_id="unit_operator",
    unit_params={
        "min_power": 200,
        "max_power": 5000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "nuclear",
    },
    forecaster=nuclear_forecast,
)


# Number of agents
num_agents = 10000

# Run simulation
world.run()