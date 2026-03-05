#!/usr/bin/env python3
"""
Experiment Runner: reads experiments.csv → configures all nodes → runs YCSB → kills replicas.
Resides at: ./BFT-SMART/build/install/library/config/run_experiments.py
"""

import os
import csv
import re
import time
import paramiko

# Script is at: ./BFT-SMART/build/install/library/config/
# ycsbClient.sh is at: ./BFT-SMART/build/install/library/
SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
YCSB_CLIENT     = os.path.join(SCRIPT_DIR, '..', 'ycsbClient.sh')
UPDATE_CONFIG   = 'BFT-Smart/build/install/library/config/update_system_config.py'
START_REPLICA   = 'BFT-Smart/build/install/library/startReplicaYCSB.sh'
RESULTS_CSV     = os.path.join(SCRIPT_DIR, 'experiments_result.csv')

# Results CSV columns
RESULT_COLUMNS = [
    'run_id', 'batchtimeout', 'maxbatchsize', 'numrepliers',
    'readproportion', 'updateproportion', 'hotspotdatafraction',
    'hotspotopnfraction', 'threads',
    'throughput_ops_sec',
    'read_operations', 'read_avg_latency_us', 'read_min_latency_us', 'read_max_latency_us',
    'update_operations', 'update_avg_latency_us', 'update_min_latency_us', 'update_max_latency_us'
]

# Node host IDs (hardcoded)
NODES = {
    'node-0': 'clnode154.clemson.cloudlab.us',
    'node-1': 'clnode167.clemson.cloudlab.us',
    'node-2': 'clnode178.clemson.cloudlab.us',
    'node-3': 'clnode153.clemson.cloudlab.us',
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
    bg_cmd = f"nohup bash -c '{cmd}' > /tmp/replica_{node_name}.log 2>&1 & echo $!"
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


def parse_ycsb_output(ycsb_output_file):
    """
    Parse YCSB output file and extract key metrics.
    Returns dict with 9 metrics, or None values if parsing fails.
    """
    metrics = {
        'throughput_ops_sec':     None,
        'read_operations':        None,
        'read_avg_latency_us':    None,
        'read_min_latency_us':    None,
        'read_max_latency_us':    None,
        'update_operations':      None,
        'update_avg_latency_us':  None,
        'update_min_latency_us':  None,
        'update_max_latency_us':  None,
    }

    if not os.path.exists(ycsb_output_file):
        print(f"  ⚠️  YCSB output file not found: {ycsb_output_file}")
        return metrics

    patterns = {
        'throughput_ops_sec':    r'\[OVERALL\],\s*Throughput\(ops/sec\),\s*([\d.]+)',
        'read_operations':       r'\[READ\],\s*Operations,\s*([\d.]+)',
        'read_avg_latency_us':   r'\[READ\],\s*AverageLatency\(us\),\s*([\d.]+)',
        'read_min_latency_us':   r'\[READ\],\s*MinLatency\(us\),\s*([\d.]+)',
        'read_max_latency_us':   r'\[READ\],\s*MaxLatency\(us\),\s*([\d.]+)',
        'update_operations':     r'\[UPDATE\],\s*Operations,\s*([\d.]+)',
        'update_avg_latency_us': r'\[UPDATE\],\s*AverageLatency\(us\),\s*([\d.]+)',
        'update_min_latency_us': r'\[UPDATE\],\s*MinLatency\(us\),\s*([\d.]+)',
        'update_max_latency_us': r'\[UPDATE\],\s*MaxLatency\(us\),\s*([\d.]+)',
    }

    with open(ycsb_output_file, 'r') as f:
        content = f.read()

    for key, pattern in patterns.items():
        match = re.search(pattern, content)
        if match:
            metrics[key] = float(match.group(1))
            print(f"  📊 {key}: {metrics[key]}")
        else:
            print(f"  ⚠️  Could not parse: {key}")

    return metrics


def write_result_row(row, metrics):
    """Append one result row to experiments_result.csv."""
    file_exists = os.path.exists(RESULTS_CSV)

    with open(RESULTS_CSV, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)

        # Write header only once
        if not file_exists:
            writer.writeheader()

        result_row = {
            'run_id':                row['run_id'],
            'batchtimeout':          row['batchtimeout'],
            'maxbatchsize':          row['maxbatchsize'],
            'numrepliers':           row['numrepliers'],
            'readproportion':        row['readproportion'],
            'updateproportion':      row['updateproportion'],
            'hotspotdatafraction':   row['hotspotdatafraction'],
            'hotspotopnfraction':    row['hotspotopnfraction'],
            'threads':               row['threads'],
            **metrics
        }
        writer.writerow(result_row)

    print(f"  ✅ Result written to experiments_result.csv")


def run_experiment(row, ssh_clients):
    """
    Run one experiment row:
    1. Update config on all nodes
    2. Start replicas on all nodes
    3. Run YCSB client locally
    4. Parse YCSB output and write result row
    """
    run_id  = row['run_id']
    bt      = row['batchtimeout']
    mbs     = row['maxbatchsize']
    nr      = row['numrepliers']
    rp      = row['readproportion']
    up      = row['updateproportion']
    hdf     = row['hotspotdatafraction']
    hof     = row['hotspotopnfraction']
    threads = row['threads']

    print(f"\n{'='*60}")
    print(f"RUN: {run_id} | bt={bt} mbs={mbs} nr={nr} threads={threads}")
    print(f"     read={rp} update={up} hdf={hdf} hof={hof}")
    print(f"{'='*60}")

    # --- STEP 1: Update config on all nodes ---
    print("\n[1/3] Updating configs on all nodes...")
    for node_name, client in ssh_clients.items():
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
        replica_cmd = (
            f"cd ~/BFT-Smart/build/install/library && "
            f"./startReplicaYCSB.sh {node_id}"
        )
        print(f"  [{node_name}] Starting replica: {replica_cmd}")
        pids[node_name] = ssh_run_background(client, replica_cmd, node_name)

    time.sleep(15)  # Give replicas time to start and form quorum

    # --- STEP 3: Run YCSB client locally ---
    print(f"\n[3/3] Running YCSB client (threads={threads}, run_id={run_id})...")
    ycsb_cmd = f"bash {YCSB_CLIENT} {threads} {run_id}"
    exit_code = os.system(ycsb_cmd)
    print(f"  YCSB exited with code: {exit_code}")

    # --- STEP 4: Parse output and write result row ---
    print(f"\n[4/4] Parsing YCSB output...")
    # Find the output file: ycsb_results_{threads}threads_{run_id}_*.txt
    ycsb_dir = os.path.join(SCRIPT_DIR, '..')
    ycsb_output_file = None
    for f in os.listdir(ycsb_dir):
        if f.startswith(f"ycsb_results_{threads}threads_{run_id}_") and f.endswith('.txt'):
            ycsb_output_file = os.path.join(ycsb_dir, f)
            break

    if ycsb_output_file:
        metrics = parse_ycsb_output(ycsb_output_file)
    else:
        print(f"  ⚠️  Could not find YCSB output file for run {run_id}")
        metrics = {k: None for k in [
            'throughput_ops_sec', 'read_operations', 'read_avg_latency_us',
            'read_min_latency_us', 'read_max_latency_us', 'update_operations',
            'update_avg_latency_us', 'update_min_latency_us', 'update_max_latency_us'
        ]}

    write_result_row(row, metrics)

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
    csv_path = os.path.join(SCRIPT_DIR, 'experiments.csv')

    print(f"Reading: {csv_path}")
    with open(csv_path, newline='') as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    print(f"Found {total} experiments to run.")

    print("\nConnecting to nodes...")
    ssh_clients = {}
    for node_name, hostname in NODES.items():
        ssh_clients[node_name] = get_ssh_client(hostname)

    prev_pids = None

    try:
        for i, row in enumerate(rows):
            if prev_pids is not None:
                kill_all_replicas(ssh_clients, prev_pids)

            prev_pids = run_experiment(row, ssh_clients)

        if prev_pids is not None:
            kill_all_replicas(ssh_clients, prev_pids)

        print("\n🎉 All experiments complete!")
        print(f"📄 Results saved to: {RESULTS_CSV}")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Cleaning up...")
        if prev_pids:
            kill_all_replicas(ssh_clients, prev_pids)

    finally:
        for client in ssh_clients.values():
            client.close()
        print("SSH connections closed.")
