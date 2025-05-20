import os
import time
import psutil
import yappi
import tracemalloc

import cProfile
import time
import pstats

from assume.world import World
from assume.scenario.loader_csv import (load_scenario_folder)
from simulationConfig import runConfig

from memory_profiler import profile


import time
import tracemalloc


#@profile
def run_simulation(num_agents):

    # CONFIG
    db_uri = "sqlite:///sim/sim.db"
    path = "./sim/units"

    os.makedirs(path, exist_ok=True)

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


def analyze_tracemalloc(agents: int):
    def format_size(size_bytes):
        return f"{size_bytes / 1024:.2f} KiB"

    def analyze_memory(snapshot_before, snapshot_after, top_n=10):
        print(f"\n🔍 Top {top_n} memory differences by traceback:")
        stats = snapshot_after.compare_to(snapshot_before, 'traceback')
        for stat in stats[:top_n]:
            print(f"\n📌 {format_size(stat.size_diff)} in {stat.count_diff} blocks")
            for line in stat.traceback.format():
                print(f"  {line}")

        print(f"\n📍 Top {top_n} allocations by file and line:")
        stats_by_line = snapshot_after.statistics('lineno')
        for stat in stats_by_line[:top_n]:
            print(f"{format_size(stat.size)}: {stat.traceback}")

    print("🚀 Starting memory profiling...")
    tracemalloc.start(25)  # 25 frames of stack for deeper tracebacks

    snapshot_before = tracemalloc.take_snapshot()
    start_time = time.time()

    run_simulation(agents)

    elapsed = time.time() - start_time
    snapshot_after = tracemalloc.take_snapshot()

    current, peak = tracemalloc.get_traced_memory()
    print(f"\n⏱️  Simulation time: {elapsed:.2f} sec")
    print(f"📊 Current memory usage: {format_size(current)}")
    print(f"📈 Peak memory usage: {format_size(peak)}")

    analyze_memory(snapshot_before, snapshot_after)

def main():

    # Just run normally: (no simulation with 1 agent)
    run_simulation(10)
    # simulation test with 10 agent of the sorted function when deltas is shuffled =
    # 10 - 156 s, 20 - 197, 30 - 241, 50 - 319, 100 - 547, test 200 - here
    # opt =
    # 10 - 156, 20 - 203, 30 - 244 s, 50 - 318, 100 - 535, test 200

    # opt : 10 - 81, 20 - 95, 50 - 153, 100 - 249, 200 - 448/454/443 450
    # no : 10 - 135/82, 20 - 99, 50 - 156, 100 - 250, 200 - 482/473/474 475
    # simulation run with optimization is 10 - 154 s, 20 - 194 / 188, 30 - 248, 50 - 323
    # without 10 - 154 s, 20 - 219 / 191, 30 - 241, 50 - 318
    # -> to test on a faster computer for better results and verify with yappi

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
    # analyze_yappi("WALL", 10, "p_opt_calCash_10_wall")


    # cProfiler
    # analyze_cProfile(10, "cProfile")

    # tracemalloc
    # analyze_tracemalloc(10)


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