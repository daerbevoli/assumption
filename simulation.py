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


import time
import tracemalloc


def run_simulation(num_agents: int, memoryProfile: bool):

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

    if not memoryProfile:
        # RUN simulation
        start = time.perf_counter()
        world.run()
        end = time.perf_counter()
        print(f"Simulation time: {end - start:.3f} sec")
    else:
        # tracemalloc
        tracemalloc.start(1)
        # RUN simulation
        start = time.perf_counter()
        world.run()
        end = time.perf_counter()
        print(f"Simulation time: {end - start:.3f} sec")
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current: {current / 10 ** 6:.6f} MB; Peak: {peak / 10 ** 6:.6f} MB")
        tracemalloc.stop()


def analyze_yappi(type: str, agents: int, saveProfile: str):
    yappi.set_clock_type(type)
    with yappi.run():
        run_simulation(agents, False)
    # p = profile, WALL, nc = no changes, 10 = agents
    yappi.get_func_stats().save("profiles/" + saveProfile + ".prof", type="pstat")
    # 10 agents = 440 s


def analyze_cProfile(agents: int, saveProfile: str):
    profiler = cProfile.Profile()
    profiler.enable()

    run_simulation(agents, False)

    profiler.disable()

    profiler.dump_stats("profiles/" + saveProfile + ".prof")

    stats = pstats.Stats(profiler)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()


def analyze_tracemalloc(agents: int):
    def format_size(size_bytes):
        return f"{size_bytes / 1024:.2f} KiB"

    def analyze_memory(snapshot_before, snapshot_after, top_n=10):
        print(f"\n Top {top_n} memory differences by traceback:")
        stats = snapshot_after.compare_to(snapshot_before, 'traceback')
        for stat in stats[:top_n]:
            print(f"\n{format_size(stat.size_diff)} in {stat.count_diff} blocks")
            for line in stat.traceback.format():
                print(f"  {line}")

        print(f"\nTop {top_n} allocations by file and line:")
        stats_by_line = snapshot_after.statistics('lineno')
        for stat in stats_by_line[:top_n]:
            print(f"{format_size(stat.size)}: {stat.traceback}")

    print("Starting memory profiling...")
    tracemalloc.start()

    snapshot_before = tracemalloc.take_snapshot()
    start_time = time.time()

    run_simulation(agents, False)

    elapsed = time.time() - start_time
    snapshot_after = tracemalloc.take_snapshot()

    current, peak = tracemalloc.get_traced_memory()
    print(f"\nSimulation time: {elapsed:.2f} sec")
    print(f"Current memory usage: {format_size(current)}")
    print(f"Peak memory usage: {format_size(peak)}")

    analyze_memory(snapshot_before, snapshot_after)

def main():

    # Just run normally: (no simulation with 1 agent)
    run_simulation(10, False)


    # yappi time profiling
    # analyze_yappi("WALL", 10, "p_opt_calCash_10_wall")


    # cProfiler
    # analyze_cProfile(10, "cProfile")

    # tracemalloc
    # analyze_tracemalloc(100)



if __name__ == "__main__":
    main()

