from multiprocessing import Pool, Pipe, Process
import multiprocessing
import requests
import time
import ray

#   ========================================================================
#                       Table of Contents
#   ========================================================================
#  1. globals
#  2. idk yet
#  3. Fork
#  4. Spawn
#  5. Pool
#  n. Main <- always last
#
#
#   ========================================================================
#                       Description
#   ========================================================================
# This module tests the different speeds of fork, spawn, and pool
#
#
#

#   ========================================================================
#                       Globals
#   ========================================================================

FULL_BAR = "=============="
LEADER = "[*]"
RUNS = 10

SPAWN = "spawn"
POOL = "pool"
FORK = "fork"
RAY = "ray"

DEFAULT_POOLS = 2

#   ========================================================================
#                       IDK yet lol
#   ========================================================================

def instant_operation(_i):
    return 1 + 1

def linear_operation(i):
    value = 0
    for _ in range(i):
        value += 1
    return value

def network_operation(_i=0):
    return requests.get("https://google.com")

#   ========================================================================
#                       Ray
#   ========================================================================

@ray.remote
def ray_instant(i):
    instant_operation(i)

@ray.remote
def ray_linear(i):
    linear_operation(i)

@ray.remote
def ray_net(_i):
    network_operation()

def ray_it(i, op=ray_net):
    futures = op.remote(i)
    ray.get(futures)

#   ========================================================================
#                       Fork
#   ========================================================================

def fork_instant(i, pipe):
    pipe.send(instant_operation(i))

def fork_linear(i, pipe):
    pipe.send(linear_operation(i))

def fork_net(_i, pipe):
    pipe.send(network_operation())

def fork_it(i, op=fork_net):
    work = list(range(i))
    context = multiprocessing.get_context("fork")
    pairs, processes = build_pipes(context, work, op)
    start_up(pairs, processes)
    close_up(pairs, processes)


#   ========================================================================
#                       Spawn
#   ========================================================================

def spawn_instant(i, pipe):
    pipe.send(instant_operation(i))

def spawn_linear(i, pipe):
    pipe.send(linear_operation(i))

def spawn_net(_i, pipe):
    pipe.send(network_operation())

def spawn_it(i, op=spawn_net):
    work = list(range(i))
    context = multiprocessing.get_context("spawn")
    pairs, processes = build_pipes(context, work, op)
    start_up(pairs, processes)
    close_up(pairs, processes)

#   ========================================================================
#                       Pool
#   ========================================================================

def pool_it(i, op=network_operation):
    data = list(range(i))
    with Pool(DEFAULT_POOLS) as p:
        p.map(op, data)

#   ========================================================================
#                       Pipeline
#   ========================================================================

def build_pipes(context, work, op):
    pipe_pairs = []
    processes = []

    for job in work:
        parent, child = context.Pipe()
        process = context.Process(
            target=op,
            args=(job, child)
        )
        pipe_pairs.append((parent, child))
        processes.append(process)

    return pipe_pairs, processes

def start_up(pairs, processes):
    for process in processes:
        process.start()

    for parent, child in pairs:
        multiprocessing.connection.wait([parent])
        data = parent.recv()

def close_up(pairs, processes):
    for process in processes:
        process.join()
        process.close()

    for parent, child in pairs:
        parent.close()

#   ========================================================================
#                       Util
#   ========================================================================

def mil_to_sec(mil):
    return mil / 0.001

def print_times(types):
    for thing in types:
        print(f"{LEADER} {thing}")
        average = types[thing][1]
        print(f"{LEADER} average: {average}")
        print("\n\n")

def run(types, RUNS):
    for _ in range(RUNS):
        for thing in types:
            start = time.time()
            types[thing][0](1)
            end = time.time()
            stamp = mil_to_sec(end - start)

            running_average = stamp / RUNS
            types[thing][1] += running_average

#   ========================================================================
#                       Main
#   ========================================================================


if __name__ == "__main__":

    types = {
        SPAWN: [spawn_it, 0],
        FORK: [fork_it, 0],
        POOL: [pool_it, 0]
    }

    print(f"{FULL_BAR} Testing Start {FULL_BAR}")
    total_time_start = time.time()

    run(types, RUNS)
    print_times(types)

    types = {
        RAY: [ray_it, 0]
    }
    ray.init()

    run(types, RUNS)
    print_times(types)

    total_time_end = time.time()
    total_time = mil_to_sec(total_time_end - total_time_start)

    print(f"{FULL_BAR} total_time: {total_time} sec {FULL_BAR}")
