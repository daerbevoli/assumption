import logging
import random
import time
from datetime import datetime, timedelta

from dateutil import rrule as rr

import pandas as pd

from assume import World
from assume.common.forecasts import NaiveForecast
from assume.common.market_objects import MarketConfig, MarketProduct

from loadCsv import loadCsv
from loadDataFluvius import loadFluviusData


log = logging.getLogger(__name__)

db_uri = "sqlite:///local_db/assume_db.db"

world = World(database_uri=db_uri)

start = datetime(2022, 1, 1)
end = datetime(2022, 12, 31, 23, 45, 00)
index = pd.date_range(
    start=start,
    end=end,
    freq="15min",
)
sim_id = "sim"

world.setup(
    start=start,
    end=end,
    save_frequency_hours=24,
    simulation_id=sim_id,
    index=index,
)


# Configuration of the electricity market
# Day Ahead market
# 1 hour window to place bid for every hour of the nex day
marketConf = MarketConfig(
        # Energy Only Market
        market_id="EOM",
        # open everyday from start to end with a 24 hour interval
        opening_hours=rr.rrule(rr.HOURLY, interval=24, dtstart=start, until=end),
        # Open for one hour (to buy and sell)
        opening_duration=timedelta(hours=1),
        # clearing mechanism -> uniform price for buyers and sells at the conclusion of the auction
        market_mechanism="pay_as_clear",
        market_products=[MarketProduct(timedelta(hours=1), 24, timedelta(hours=1))],
        maximum_bid_volume=20000, # choose the value wisely
        maximum_bid_price=15000,

        additional_fields=["block_id", "link", "exclusive_id"],
    )


mo_id = "Electricity_market"
world.add_market_operator(id=mo_id)

world.add_market(market_operator_id=mo_id, market_config=marketConf)

# Setting up agent0
# Remove unnecessary columns
columns_to_remove = ['Datetime','Resolution code', 'Most recent P10', 'Most recent P90', 'Day-ahead 6PM forecast',
       'Day-ahead 6PM P10', 'Day-ahead 6PM P90', 'Most recent forecast', 'Week-ahead forecast']

# load the dataframe
df = loadCsv('MeasuredForecastedLoadAgent0.csv', columns_to_remove, index)

# Set up agent0 unit
world.add_unit_operator("agent0_operator")

# Link agent0 list with forecaster
agent0_forecast = NaiveForecast(index, demand=df['Total Load'])

world.add_unit(
    id="demand_unit", # YOU CANNOT CHANGE THE ID TO ANYTHING OTHER THAN DEMAND OR TYPE OF POWER UNIT
    unit_type="demand",
    unit_operator_id="agent0_operator",
    unit_params={
        "min_power": 0,
        "max_power": 10000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "demand",
    },
    forecaster=agent0_forecast,
)

# setting up dummy agents with Fluvius data
loads, feeds = loadFluviusData(10)

# Set up load unit
world.add_unit_operator("load_operator")

# Link load list with forecaster
load_forecast = NaiveForecast(index, demand=random.choice(loads))

print(load_forecast.__getitem__("demand"))

world.add_unit(
    id="resident_load",
    unit_type="demand", # YOU CANNOT CHANGE THE TYPE TO ANYTHING OTHER THAN DEMAND OR TYPE OF POWER UNIT
    unit_operator_id="load_operator",
    unit_params={
        "min_power": 0,
        "max_power": 10000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "demand",
    },
    forecaster=load_forecast,
)

# Set up feedin unit as producer unit
world.add_unit_operator("feedin_operator")

feedin_forecast = NaiveForecast(index, availability=1, fuel_price=3, co2_price=0.1, demand=random.choice(feeds))

print(feedin_forecast.__getitem__("demand"))

world.add_unit(
    id="resident_feedin",
    unit_type="power_plant",
    unit_operator_id="feedin_operator",
    unit_params={
        "min_power": 100,
        "max_power": 10000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "solar",
    },
    forecaster=feedin_forecast,
)


# Set up producer unit
world.add_unit_operator("power_operator")

nuclear_forecast = NaiveForecast(index, availability=1, fuel_price=3, co2_price=0.1)

world.add_unit(
    id="nuclear_unit",
    unit_type="power_plant",
    unit_operator_id="power_operator",
    unit_params={
        "min_power": 100,
        "max_power": 10000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "nuclear",
    },
    forecaster=nuclear_forecast,
)


# Time the simulation
start_time = time.perf_counter()

# Run simulation
world.run()

end_time = time.perf_counter()

print(f"Execution time of simulation: {end_time - start_time:.6f} seconds")

# timeit
# python profilers time
# tracemalloc

