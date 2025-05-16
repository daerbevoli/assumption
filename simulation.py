import os
import time
import psutil
import yappi
import tracemalloc

import cProfile
import time
import pstats

from assume import World
from assume.scenario.loader_csv import load_scenario_folder
from simulationConfig import runConfig

from memory_profiler import profile


import time
import tracemalloc

#@profile
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

def analyze_yappi(type: str, agents: int, saveProfile: str):
    yappi.set_clock_type(type)
    with yappi.run():
        run_simulation(agents)
    # p = profile, WALL, nc = no changes, 10 = agents
    yappi.get_func_stats().save("profiles/" + saveProfile + ".prof", type="pstat")
    # 10 agents = 440 s


def analyze_cProfile(agents: int, saveProfile: str):
    profiler = cProfile.Profile()
    profiler.enable()

    run_simulation(agents)

    profiler.disable()

    profiler.dump_stats("profiles/" + saveProfile + ".prof")

    stats = pstats.Stats(profiler)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()

def main():

    # Just run normally: (no simulation with 1 agent)
    run_simulation(10)
    # error running
    # wc
    # 5 = 77 s
    # 10 = 84 s
    # 20 = 104 s
    # 50 = 154 s
    # 100 = 269 s
    # 500 = 1012 s
    # 1000

    # yappi time profiling
    #analyze_yappi("WALL", 10, "p_nc_10_wall")

    # cProfiler
    # analyze_cProfile(10, "cProfile")

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