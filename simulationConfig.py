
import pandas as pd
import yaml


from loadDataFluvius import loadFluviusData
from loadCsv import loadCsv

def save_powerplant_units(path):
    powerplant_units_data = {
        "name": ["nuclear", "lignite", "coal", "gas", "ResidentSolar"],
        "technology": ["nuclear", "lignite", "hard coal", "combined cycle gas turbine", "solar"],
        "bidding_EOM": ["naive_eom"] * 5,
        "fuel_type": ["uranium", "lignite", "hard coal", "natural gas", "solar"],
        "emission_factor": [0.0, 0.4, 0.3, 0.2, 0.0],
        "max_power": [1000.0, 8000.0, 10.0, 100.0, None],
        "min_power": [200.0, 200.0, 1.0, 10.0, 0.0],
        "efficiency": [0.3, 0.5, 0.4, 0.6, 1.0],
        "additional_cost": [10.3, 1.65, 1.3, 3.5, 0],
        "unit_operator": [f"Operator {i+1}" for i in range(5)],
    }
    df = pd.DataFrame(powerplant_units_data)
    df.to_csv(f"{path}/powerplant_units.csv", index=False)

def save_fuel_prices(path):
    fuel_prices_data = {
        "fuel": ["uranium", "lignite", "hard coal", "natural gas", "oil", "biomass", "co2"],
        "price": [1, 2, 10, 25, 40, 20, 25],
    }
    df = pd.DataFrame(fuel_prices_data)
    df.to_csv(f"{path}/fuel_prices.csv", index=False)


def save_demand_units(path: str, num_agents: int):
    # Load Agent0
    columns_to_remove = ['Datetime', 'Resolution code', 'Most recent P10', 'Most recent P90', 'Day-ahead 6PM forecast',
                         'Day-ahead 6PM P10', 'Day-ahead 6PM P90', 'Most recent forecast', 'Week-ahead forecast']
    agent0 = loadCsv('MeasuredForecastedLoadAgent0.csv', columns_to_remove)

    # Load Fluvius meter data
    meters = loadFluviusData(num_agents - 1)  # Load data for the other agents

    # Create demand unit data
    demand_units_data = {
        "name": ["Agent0"] + [f"Resident{i + 1}" for i in range(num_agents - 1)],
        "technology": ["demand"] * num_agents,
        "bidding_EOM": ["naive_eom"] * num_agents,
        "max_power": [10000] + [1] * (num_agents - 1),  # Assign random max power
        "min_power": [0] * num_agents,
        "unit_operator": ["eom_be"] * num_agents,
    }

    # Convert to DataFrame and save
    demand_units_df = pd.DataFrame(demand_units_data)
    demand_units_df.to_csv(f"{path}/demand_units.csv", index=False)

    # Prepare demand data
    agents_demand = pd.DataFrame()
    agents_demand["Agent0"] = agent0["Total Load"]

    # Add demand from Fluvius meters
    for i, meter in enumerate(meters):
        agents_demand[f"Resident{i + 1}"] = meter["load"]

    # Save demand data to CSV
    agents_demand.to_csv(f"{path}/demand_df.csv", index=True)
    print(f"Added {num_agents} agents and saved demand data.")



def save_config(path):
    config_data = {
        "Day_Ahead_market": {
            "start_date": "2022-01-01 00:00:00",
            "end_date": "2022-12-31 23:45:00",
            "time_step": "1h",
            "save_frequency_hours": 24,
            "markets_config": {
                "EOM": {
                    "operator": "EOM_operator",
                    "product_type": "energy",
                    "opening_frequency": "1h",
                    "opening_duration": "1h",
                    "products": [{"duration": "1h", "count": 24, "first_delivery": "0h"}],
                    "volume_unit": "MW",
                    "price_unit": "EUR/MW",
                    "market_mechanism": "pay_as_clear",
                }
            },
        }
    }
    with open(f"{path}/config.yaml", "w") as file:
        yaml.dump(config_data, file, sort_keys=False)

def runConfig(input_path: str, num_agents: int):
    save_powerplant_units(input_path)
    save_fuel_prices(input_path)
    save_demand_units(input_path, num_agents)
    save_config(input_path)
