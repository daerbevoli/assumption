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
    #run_simulation(10)

    # OR: run with yappi profiling
    #yappi.set_clock_type("WALL")
    #with yappi.run():
    #     run_simulation(10)
    # p = profile, WALL, nc = no changes, 10 = agents
    #yappi.get_func_stats().save("profiles/p_WALL_cashflow_10.prof", type="pstat")

if __name__ == "__main__":
    main()

# Execution time of simulation of year with 10 agents: 217, 218 s
# Execution time of simulation of year with 10 agents speedup cashflow: 225, 216, 217 s

# Execution time of simulation of year with 15 agents: 311 s

# Execution time of simulation of year with 20 agents: 318 s
# Execution time of simulation of year with 20 agents speedup cashflow: 331 s


# yappi generates significant amount overhead

# 20 agent, 1 year, 1 - 2 - 3 - 4 core = 357 s - 344 s - 333 - 348
# 3 cores is optimal