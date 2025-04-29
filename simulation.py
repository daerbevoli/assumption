import os
import time
import psutil
import yappi

from assume import World
from assume.scenario.loader_csv import load_scenario_folder
from simulationConfig import runConfig

def run_simulation(num_agents):

    # CONFIG
    db_uri = "sqlite:///sim/sim.db"
    path = "./sim/units"

    # Prepare data
    start = time.perf_counter()
    runConfig(path, num_agents)
    print(f"runConfig time: {time.perf_counter() - start:.3f} sec")

    # Init World
    start = time.perf_counter()
    world = World(database_uri=db_uri, export_csv_path="./sim")
    print(f"World creation time: {time.perf_counter() - start:.3f} sec")

    # Load Scenario
    start = time.perf_counter()
    load_scenario_folder(world, inputs_path="./sim", scenario="units", study_case="Day_Ahead_market")
    print(f"Scenario load time: {time.perf_counter() - start:.3f} sec")

    # RUN simulation
    start = time.perf_counter()
    world.run()
    end = time.perf_counter()
    print(f"Simulation time: {end - start:.3f} sec")


def main():
    # Just run normally: (no simulation with 1 agent)
    # run_simulation(5)

    # OR: run with yappi profiling
    #yappi.set_clock_type("WALL")
    #with yappi.run():
    #     run_simulation(10)
    # p = profile, WALL, nc = no changes, 10 = agents
    #yappi.get_func_stats().save("profiles/p_WALL_nc_10.prof", type="pstat")

if __name__ == "__main__":
    main()

# bottleneck 1: aggregate_step_amount
# Test 1 : SortedList from sortedContainers -> slower by about 1.5
# Test 2 : SortedKeyList -> even slower
# QUESTION : Do I need to add all the attempts to my thesis or in a different report?
# Test 3 : vectorization -> super slow


# Test 4 : calculate_cashflow optimization -> pretty much same time

# yappi generates significant amount overhead

# 20 agent, 1 year, 1 - 2 - 3 - 4 core = 357 s - 344 s - 333 - 348
# 3 cores is optimal