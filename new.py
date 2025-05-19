import logging
import random
import time
from datetime import datetime, timedelta

from dateutil import rrule as rr

import pandas as pd

from assume.assume import World
from assume.assume.common.forecasts import NaiveForecast
from assume.assume.common.market_objects import MarketConfig, MarketProduct

from loadFiles.loadCsv import loadCsv
from loadFiles.loadDataFluvius import loadFluviusData


log = logging.getLogger(__name__)

db_uri = "sqlite:///sim/assume_db.db"

world = World(database_uri=db_uri)

start = datetime(2022, 1, 1)
end = datetime(2022, 12, 31, 23, 45, 00)
index = pd.date_range(
    start=start,
    end=end,
    freq="h",
)

sim_id = "sim_main"

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
        # open every day from start to end with a 24 hour interval
        opening_hours=rr.rrule(rr.HOURLY, interval=24, dtstart=start, until=end),
        # Open for one hour (to buy and sell)
        opening_duration=timedelta(hours=1),
        # clearing mechanism -> uniform price for buyers and sells at the conclusion of the auction
        market_mechanism="pay_as_clear",
        market_products=[MarketProduct(timedelta(minutes=60), 24, timedelta(minutes=0))], # per kwartier
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
df = loadCsv('MeasuredForecastedLoadAgent0.csv', columns_to_remove)

print(df['Total Load'])

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
        "max_power": 10000, # agent0 consumes a lot of energy
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "demand",
        "price": 4,
    },
    forecaster=agent0_forecast,
)


# setting up dummy agents with Fluvius data
meters = loadFluviusData(1)

random_meter = random.choice(meters)

world.add_unit_operator("load_operator")

# Time the simulation
start_time = time.perf_counter()

# Run simulation
print(random_meter.head(20))

end_time = time.perf_counter()

print(f"Execution time of simulation: {end_time - start_time:.6f} seconds")


# Link load list with forecaster
load_forecast = NaiveForecast(index, demand=random_meter['load'])

world.add_unit(
    id="demand_unit", # If this change, it does not participate in the market? Very trivial
    unit_type="demand", # YOU CANNOT CHANGE THE TYPE TO ANYTHING OTHER THAN DEMAND OR TYPE OF POWER UNIT
    unit_operator_id="load_operator",
    unit_params={
        "min_power": 0,
        "max_power": 10000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "demand",
        "price": 4,
    },
    forecaster=load_forecast,
)
'''
# Set up feedin unit as producer unit
world.add_unit_operator("feedin_operator")

feedin_forecast = NaiveForecast(index, availability=1, fuel_price=3, co2_price=0.1, demand=random_meter['feedin'])

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
'''

# Set up producer unit
world.add_unit_operator("power_operator")

nuclear_forecast = NaiveForecast(index, availability=1, fuel_price=3, co2_price=0.1)

world.add_unit(
    id="nuclear_unit",
    unit_type="power_plant",
    unit_operator_id="power_operator",
    unit_params={
        "min_power": 10,
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

# DOUBLE CHECK UNITS ELIA FLUVIUS AND FRAMEWORK
# PROFILEN


"""
First consumer places his bid, x demand for x price and then producer places his offer, 10000 MW for 3 
market order is first consumer in minus then generator in +
market dispatch is actual buying after clearing the market
"""
