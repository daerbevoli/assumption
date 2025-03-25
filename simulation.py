from assume import World
from assume.scenario.loader_csv import load_scenario_folder

import time

from simulationConfig import runConfig

# define the path to save config data and the number of total agents
path = "./data/units"
num_agents = 10

# run config to save data
start_time = time.perf_counter()
runConfig(path, num_agents)
end_time = time.perf_counter()

print(f"Execution time of runConfig: {end_time - start_time:.6f} seconds")


# define the database uri. In this case we are using a local sqlite database
db_uri = "sqlite:///local_db/example_db.db"

# create world instance
start_time = time.perf_counter()
world = World(database_uri=db_uri, export_csv_path="./data")
end_time = time.perf_counter()
print(f"Execution time of creating world: {end_time - start_time:.6f} seconds")

# load scenario by providing the world instance
# and the study case name (which config to use for the simulation)
start_time = time.perf_counter()
load_scenario_folder(
    world,
    inputs_path="./data",
    scenario="units",
    study_case="Day_Ahead_market",
)
end_time = time.perf_counter()
print(f"Execution time of loading scenario: {end_time - start_time:.6f} seconds")

# Run simulation
start_time = time.perf_counter()
world.run()
end_time = time.perf_counter()

print(f"Execution time of simulation of a month with {num_agents} agents: {end_time - start_time:.6f} seconds")
