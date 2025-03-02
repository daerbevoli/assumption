import logging
from datetime import datetime, timedelta

import pandas as pd
from dateutil import rrule as rr

from assume import World
from assume.common.forecasts import NaiveForecast
from assume.common.market_objects import MarketConfig, MarketProduct

log = logging.getLogger(__name__)

db_uri = "sqlite:///local_db/assume_db.db"

world = World(database_uri=db_uri)

start = datetime(2023, 1, 1)
end = datetime(2023, 3, 31)
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

marketdesign = [
    MarketConfig(
        market_id="EOM", # Energy Only Market
        opening_hours=rr.rrule(rr.HOURLY, interval=24, dtstart=start, until=end),
        opening_duration=timedelta(hours=1),
        market_mechanism="pay_as_clear",
        market_products=[MarketProduct(timedelta(hours=1), 24, timedelta(hours=1))],
        additional_fields=["block_id", "link", "exclusive_id"],
    )
]

mo_id = "market_operator"
world.add_market_operator(id=mo_id)

for market_config in marketdesign:
    world.add_market(market_operator_id=mo_id, market_config=market_config)

# consumer 1
world.add_unit_operator("demand_operator")
demand_forecast = NaiveForecast(index, demand=100) # demand is 100 MWh
world.add_unit(
    id="demand_unit",
    unit_type="demand",
    unit_operator_id="demand_operator",
    unit_params={
        "min_power": 0,
        "max_power": 1000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "demand",
    },
    forecaster=demand_forecast,
)

# producer 1
world.add_unit_operator("unit_operator")
nuclear_forecast = NaiveForecast(index, availability=1, fuel_price=3, co2_price=0.1)
world.add_unit(
    id="nuclear_unit",
    unit_type="power_plant",
    unit_operator_id="unit_operator",
    unit_params={
        "min_power": 200,
        "max_power": 1000,
        "bidding_strategies": {"EOM": "naive_eom"},
        "technology": "nuclear",
    },
    forecaster=nuclear_forecast,
)

world.run()