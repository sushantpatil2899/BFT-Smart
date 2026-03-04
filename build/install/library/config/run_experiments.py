#!/usr/bin/env python3
"""
Experiment Runner: reads experiments.csv → configures all nodes → runs YCSB → kills replicas.
Resides at: ./BFT-SMART/build/install/library/config/run_experiments.py
"""

import os
import csv
import time
import paramiko

# Script is at: ./BFT-SMART/build/install/library/config/
# ycsbClient.sh is at: ./BFT-SMART/build/install/library/
SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
YCSB_CLIENT     = os.path.join(SCRIPT_DIR, '..', 'ycsbClient.sh')
UPDATE_CONFIG   = 'BFT-Smart/build/install/library/config/update_system_config.py'
START_REPLICA   = 'BFT-Smart/build/install/library/startReplicaYCSB.sh'

# Node host IDs (hardcoded)
NODES = {
    'node-0': 'clnode156.clemson.cloudlab.us',  # Replace with actual hostnames
    'node-1': 'clnode179.clemson.cloudlab.us',
    'node-2': 'clnode164.clemson.cloudlab.us',
    'node-3': 'clnode145.clemson.cloudlab.us',
}

def get_ssh_client(hostname):
    """Connect to node via SSH key-only auth."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    username = "sush99"
    try:
        client.connect(hostname, username=username, look_for_keys=True, timeout=15)
        print(f"  ✅ Connected to {hostname} as {username}")
        return client
    except paramiko.AuthenticationException:
        raise Exception(f"❌ Could not connect to {hostname}")

def ssh_run_blocking(client, cmd, node_name):
    """Run command and wait for it to finish. Returns stdout."""
    _, stdout, stderr = client.exec_command(cmd)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    if out:
        print(f"  [{node_name}] {out}")
    if err:
        print(f"  [{node_name}] ERR: {err}")
    return exit_code, out

def ssh_run_background(client, cmd, node_name):
    """Run command in background, return PID for later kill."""
    bg_cmd = f"nohup {cmd} > /tmp/replica_{node_name}.log 2>&1 & echo $!"
    _, stdout, _ = client.exec_command(bg_cmd)
    pid = stdout.read().decode().strip()
    print(f"  [{node_name}] Started background PID={pid}")
    return pid

def kill_replica(client, pid, node_name):
    """Kill the background replica process by PID."""
    if pid:
        _, stdout, _ = client.exec_command(f"kill {pid} 2>/dev/null; echo done")
        stdout.channel.recv_exit_status()
        print(f"  [{node_name}] Killed PID={pid}")

def run_experiment(row, ssh_clients, replica_pids):
    """
    Run one experiment row:
    1. Update config on all nodes
    2. Start replicas on all nodes
    3. Run YCSB client locally
    """
    run_id    = row['run_id']
    bt        = row['batchtimeout']
    mbs       = row['maxbatchsize']
    nr        = row['numrepliers']
    rp        = row['readproportion']
    up        = row['updateproportion']
    hdf       = row['hotspotdatafraction']
    hof       = row['hotspotopnfraction']
    threads   = row['threads']

    print(f"\n{'='*60}")
    print(f"RUN: {run_id} | bt={bt} mbs={mbs} nr={nr} threads={threads}")
    print(f"     read={rp} update={up} hdf={hdf} hof={hof}")
    print(f"{'='*60}")

    # --- STEP 1: Update config on all nodes ---
    print("\n[1/3] Updating configs on all nodes...")
    for node_name, client in ssh_clients.items():
        node_id = node_name.split('-')[1]
        update_cmd = (
            f"python3 {UPDATE_CONFIG} "
            f"{bt} {mbs} {nr} {rp} {up} {hdf} {hof}"
        )
        print(f"  [{node_name}] Running update_system_config.py...")
        exit_code, _ = ssh_run_blocking(client, update_cmd, node_name)
        if exit_code != 0:
            print(f"  ❌ Config update failed on {node_name}! Skipping run.")
            return None

    # --- STEP 2: Start replicas in background on all nodes ---
    print("\n[2/3] Starting replicas on all nodes...")
    pids = {}
    for node_name, client in ssh_clients.items():
        node_id = node_name.split('-')[1]
        # replica_cmd = f"{START_REPLICA} {node_id}"
        replica_cmd = (
            f"cd ~/BFT-Smart/build/install/library && "
            f"./startReplicaYCSB.sh {node_id}"
        )
        pids[node_name] = ssh_run_background(client, replica_cmd, node_name)

    time.sleep(5)  # Give replicas time to start

    # --- STEP 3: Run YCSB client locally ---
    print(f"\n[3/3] Running YCSB client (threads={threads}, run_id={run_id})...")
    ycsb_cmd = f"bash {YCSB_CLIENT} {threads} {run_id}"
    exit_code = os.system(ycsb_cmd)
    print(f"  YCSB exited with code: {exit_code}")

    return pids

def kill_all_replicas(ssh_clients, pids):
    """Kill replicas on all nodes."""
    if not pids:
        return
    print("\n[CLEANUP] Killing replicas...")
    for node_name, client in ssh_clients.items():
        kill_replica(client, pids.get(node_name), node_name)
    time.sleep(3)

if __name__ == "__main__":
    # Read experiments.csv from same folder as this script
    csv_path = os.path.join(SCRIPT_DIR, 'experiments.csv')

    print(f"Reading: {csv_path}")
    with open(csv_path, newline='') as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    print(f"Found {total} experiments to run.")

    # Connect to all nodes once
    print("\nConnecting to nodes...")
    ssh_clients = {}
    for node_name, hostname in NODES.items():
        ssh_clients[node_name] = get_ssh_client(hostname)

    prev_pids = None

    try:
        for i, row in enumerate(rows):
            # Kill previous replicas at start of next run
            if prev_pids is not None:
                kill_all_replicas(ssh_clients, prev_pids)

            # Run experiment
            prev_pids = run_experiment(row, ssh_clients, prev_pids)

        # Kill last run's replicas
        if prev_pids is not None:
            kill_all_replicas(ssh_clients, prev_pids)

        print("\n🎉 All experiments complete!")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Cleaning up...")
        if prev_pids:
            kill_all_replicas(ssh_clients, prev_pids)

    finally:
        for client in ssh_clients.values():
            client.close()
        print("SSH connections closed.")
