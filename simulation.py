import cProfile
import pstats

from assume import World
from assume.scenario.loader_csv import load_scenario_folder

import time
import yappi

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
db_uri = "sqlite:///local_db/sim.db"

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
'''
profiler = cProfile.Profile()
profiler.enable()



profiler.disable()
stats = pstats.Stats(profiler)
stats.strip_dirs().sort_stats("cumtime").print_stats(10)  # Shows top 10 slowest functions


stats.sort_stats("ncalls").print_stats(20)  # Show top 20 most called functions
'''

yappi.set_clock_type("WALL")
with yappi.run():
    start_time = time.perf_counter()
    world.run()
    end_time = time.perf_counter()

#yappi.get_func_stats().save("profileWALL.prof", type="pstat")

print(f"Execution time of simulation of a year with {num_agents} agents: {end_time - start_time:.6f} seconds")

# Execution time of simulation of year with 10 agents: 240 s
# Execution time of simulation of year with 15 agents: 311 s
# Execution time of simulation of year with 20 agents: 406 s

# cProfile generates immense amounts of overhead -> double the time
# yappi also generates significant amount overhead

# yappi cProfile
