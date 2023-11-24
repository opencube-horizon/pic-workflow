#!/usr/bin/env python3

import os
import time
import csv
import glob
import re
import sys

OUT_DIR = 'out/'


if len(sys.argv) < 2:
    print(f'usage: {sys.argv[0]} [sim_name0 [sim_name1 [...]]]', file=sys.stderr)
    exit(1)

# Gets center element from Ez file, hardcoded grid size
def maxSearch(file_path):
    center_row = 63
    center_col = 31

    with open(file_path, 'r') as f:
        r = csv.reader(f, delimiter=' ', quoting=csv.QUOTE_NONNUMERIC)

        # skip center_row first lines
        for i in range(center_row): next(r)

        # this is the center_row line
        return abs(next(r)[center_col])

simulations = []

# get a list of simulation directories
for sim_name in sys.argv[1:]:
    sim_dir  = os.path.join(OUT_DIR, sim_name)
    sim_path = os.path.join(sim_dir, 'sim.inp')

    # Find the ncycles for each simulation
    with open(sim_path, 'r') as f:
        for line in f:
            if not line.startswith('ncycles'): continue
            
            v = int(re.search(r'\d+', line).group())
            break

    simulations.append({
        'name': sim_name, 
        'ncycles': v,
        'dir': sim_dir,
        'ez_val': 10,
        'max_val': -1,
        'finished': False
    })

total_finished = 0
while total_finished < len(simulations):

    # check the current state of all simulations
    for sim in simulations:
        if sim['finished']: continue

        fpath = os.path.join(sim['dir'], f"Ez_{sim['ez_val']}.spic")

        # simulation is finished
        if sim['ez_val'] > sim['ncycles']:
            total_finished += 1
            sim['finished'] = True
            print(f"simulation {sim['name']} finished.")

        # wait for the corresponding file to be present
        elif os.path.isfile(fpath):
            val = maxSearch(fpath)

            if val > sim['max_val']:
                sim['max_val'] = val

            # go to next step
            sim['ez_val'] += 10

    time.sleep(1)

print([sim['ez_val'] for sim in simulations])
print([sim['max_val'] for sim in simulations])
