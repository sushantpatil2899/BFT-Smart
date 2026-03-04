#!/usr/bin/env python3
from itertools import product
import csv

batchtimeout     = [-1, 0, 1, 2]
maxbatchsize     = [1024, 2048, 3072, 4096]
numrepliers      = [16]
read_update_pairs = [(0.1, 0.9), (0.5, 0.5), (0.9, 0.1)]
hotspot_pairs    = [(0.0, 0.0), (0.2, 0.2), (0.1, 0.9)]
threads          = [500, 1000, 1500, 2000]

rows = []
run_id = 1
for bt, mbs, nr, ru, hs, th in product(
    batchtimeout, maxbatchsize, numrepliers,
    read_update_pairs, hotspot_pairs, threads
):
    rows.append({
        'run_id':              run_id,
        'batchtimeout':        bt,
        'maxbatchsize':        mbs,
        'numrepliers':         nr,
        'readproportion':      ru[0],
        'updateproportion':    ru[1],
        'hotspotdatafraction': hs[0],
        'hotspotopnfraction':  hs[1],
        'threads':             th
    })
    run_id += 1

with open('experiments.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Done! {len(rows)} rows written to experiments.csv")
