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
from pyinstrument import Profiler


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
    # run_simulation(2)

    # tracemalloc memory profiling
    # tracemalloc.start()
    # run_simulation(10)
    #
    # snapshot = tracemalloc.take_snapshot()
    # top_stats = snapshot.statistics('lineno')
    #
    # for stat in top_stats:
    #     print(stat)

    # pyinstrument time sampling profiler -> superficial
    # profiler = Profiler()
    # profiler.start()
    #
    # # Your code here
    # run_simulation(50)
    #
    # profiler.stop()
    # print(profiler.output_text(unicode=True, color=True))

    # yappi time profiling
    #yappi.set_clock_type("WALL")
    #with yappi.run():
    #     run_simulation(10)
    # p = profile, WALL, nc = no changes, 10 = agents
    #yappi.get_func_stats().save("profiles/p_WALL_nc_10.prof", type="pstat")

    # cProfile (only measures CPU time)
    # profiler = cProfile.Profile()
    # profiler.enable()
    #
    # run_simulation(10)
    #
    # profiler.disable()
    #
    # stats = pstats.Stats(profiler)
    # stats.sort_stats(pstats.SortKey.CUMULATIVE)
    # stats.print_stats()

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