#!/usr/bin/env python3
"""
Experiment Runner: reads experiments.csv → configures all nodes → runs YCSB → kills replicas.
Resides at: ./BFT-SMART/build/install/library/config/run_experiments.py
"""

import os
import csv
import re
import time
import smtplib
import paramiko
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
YCSB_CLIENT     = os.path.join(SCRIPT_DIR, '..', 'ycsbClient.sh')
UPDATE_CONFIG   = 'BFT-Smart/build/install/library/config/update_system_config.py'
START_REPLICA   = 'BFT-Smart/build/install/library/startReplicaYCSB.sh'
RESULTS_CSV     = os.path.join(SCRIPT_DIR, 'experiments_result.csv')

# Skip already-completed runs (set to 1 to run all)
START_FROM_RUN  = 1

# ─── Email config ────────────────────────────────────────────────
EMAIL_SENDER    = "sushantpatil2899@gmail.com"       # ← your Gmail
EMAIL_PASSWORD  = "zzwj utdq dvzs leom"          # ← Gmail App Password (not your login password)
EMAIL_RECEIVER  = "sushantpatil2899@gmail.com"       # ← where to receive (can be same)
SMTP_HOST       = "smtp.gmail.com"
SMTP_PORT       = 587
# ─────────────────────────────────────────────────────────────────

RESULT_COLUMNS = [
    'run_id', 'batchtimeout', 'maxbatchsize', 'numrepliers',
    'readproportion', 'updateproportion', 'hotspotdatafraction',
    'hotspotopnfraction', 'threads',
    'throughput_ops_sec',
    'read_operations', 'read_avg_latency_us', 'read_min_latency_us', 'read_max_latency_us',
    'update_operations', 'update_avg_latency_us', 'update_min_latency_us', 'update_max_latency_us'
]

NODES = {
    'node-0': 'clnode004.clemson.cloudlab.us',
    'node-1': 'clnode096.clemson.cloudlab.us',
    'node-2': 'clnode093.clemson.cloudlab.us',
    'node-3': 'clnode055.clemson.cloudlab.us',
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
    """Force kill replica shell wrapper + any lingering Java YCSBServer process."""
    if pid:
        client.exec_command(f"kill -9 {pid} 2>/dev/null")
    _, stdout, _ = client.exec_command(
        "pkill -9 -f 'bftsmart.demo.ycsb.YCSBServer' 2>/dev/null; echo done"
    )
    stdout.channel.recv_exit_status()
    print(f"  [{node_name}] Force killed replica (PID={pid})")


def wait_for_ports_free(ssh_clients, port_pattern="1100", max_wait=30):
    """Wait until BFT-SMaRt ports are released on all nodes."""
    print("  Waiting for ports to release...")
    waited = 0
    while waited < max_wait:
        all_free = True
        for node_name, client in ssh_clients.items():
            _, stdout, _ = client.exec_command(
                f"ss -tlnp | grep '{port_pattern}' | wc -l"
            )
            count = stdout.read().decode().strip()
            if count != '0':
                print(f"  ⚠️  [{node_name}] Ports still in use ({count} sockets)...")
                all_free = False
        if all_free:
            print("  ✅ All ports free!")
            return
        time.sleep(5)
        waited += 5
    print(f"  ⚠️  Ports did not fully release after {max_wait}s — proceeding anyway.")


def force_kill_all_nodes(ssh_clients):
    """Forcefully kill any YCSBServer processes on all nodes."""
    print("\n[CLEANUP] Force killing any existing YCSBServer processes...")
    for node_name, client in ssh_clients.items():
        _, stdout, _ = client.exec_command(
            "pkill -9 -f 'bftsmart.demo.ycsb.YCSBServer' 2>/dev/null; echo done"
        )
        stdout.channel.recv_exit_status()
        print(f"  [{node_name}] Cleaned up")


def kill_all_replicas(ssh_clients, pids):
    """Kill replicas on all nodes and wait for ports to be fully released."""
    if not pids:
        return
    print("\n[CLEANUP] Killing replicas...")
    for node_name, client in ssh_clients.items():
        kill_replica(client, pids.get(node_name), node_name)
    wait_for_ports_free(ssh_clients, port_pattern="1100", max_wait=30)


def send_email(subject, body, attach_csv=True):
    """Send email notification with optional experiments_result.csv attachment."""
    try:
        msg = MIMEMultipart()
        msg['From']    = EMAIL_SENDER
        msg['To']      = EMAIL_RECEIVER
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'html'))

        # Attach results CSV if it exists
        if attach_csv and os.path.exists(RESULTS_CSV):
            with open(RESULTS_CSV, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="experiments_result.csv"'
            )
            msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())

        print(f"  ✅ Email sent to {EMAIL_RECEIVER}")

    except Exception as e:
        print(f"  ⚠️  Failed to send email: {e}")


def build_summary_table(rows):
    """Build an HTML table of results for the email body."""
    if not os.path.exists(RESULTS_CSV):
        return "<p>No results file found.</p>"

    with open(RESULTS_CSV, newline='') as f:
        result_rows = list(csv.DictReader(f))

    if not result_rows:
        return "<p>Results file is empty.</p>"

    headers = result_rows[0].keys()
    header_html = "".join(f"<th style='padding:6px;border:1px solid #ccc'>{h}</th>" for h in headers)
    rows_html = ""
    for r in result_rows:
        cells = "".join(f"<td style='padding:6px;border:1px solid #ccc'>{r.get(h, '')}</td>" for h in headers)
        rows_html += f"<tr>{cells}</tr>"

    return f"""
    <table style='border-collapse:collapse;font-size:12px;font-family:monospace'>
        <thead><tr style='background:#f0f0f0'>{header_html}</tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    """


def parse_ycsb_output(ycsb_output_file):
    """Parse YCSB output file and extract key metrics."""
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
        if not file_exists:
            writer.writeheader()
        result_row = {
            'run_id':              row['run_id'],
            'batchtimeout':        row['batchtimeout'],
            'maxbatchsize':        row['maxbatchsize'],
            'numrepliers':         row['numrepliers'],
            'readproportion':      row['readproportion'],
            'updateproportion':    row['updateproportion'],
            'hotspotdatafraction': row['hotspotdatafraction'],
            'hotspotopnfraction':  row['hotspotopnfraction'],
            'threads':             row['threads'],
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
    print("\n[1/4] Updating configs on all nodes...")
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
    print("\n[2/4] Starting replicas on all nodes...")
    pids = {}
    for node_name, client in ssh_clients.items():
        node_id = node_name.split('-')[1]
        replica_cmd = (
            f"cd ~/BFT-Smart/build/install/library && "
            f"./startReplicaYCSB.sh {node_id}"
        )
        print(f"  [{node_name}] Starting replica: {replica_cmd}")
        pids[node_name] = ssh_run_background(client, replica_cmd, node_name)

    time.sleep(15)

    # --- STEP 3: Run YCSB client locally ---
    print(f"\n[3/4] Running YCSB client (threads={threads}, run_id={run_id})...")
    ycsb_cmd = f"bash {YCSB_CLIENT} {threads} {run_id}"
    exit_code = os.system(ycsb_cmd)
    print(f"  YCSB exited with code: {exit_code}")

    # --- STEP 4: Parse output and write result row ---
    print(f"\n[4/4] Parsing YCSB output...")
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

    force_kill_all_nodes(ssh_clients)
    wait_for_ports_free(ssh_clients, port_pattern="1100", max_wait=30)

    prev_pids    = None
    completed    = 0
    interrupted  = False

    try:
        for i, row in enumerate(rows):
            if int(row['run_id']) < START_FROM_RUN:
                print(f"  ⏭️  Skipping run {row['run_id']} (START_FROM_RUN={START_FROM_RUN})")
                continue

            if prev_pids is not None:
                kill_all_replicas(ssh_clients, prev_pids)

            prev_pids = run_experiment(row, ssh_clients)
            completed += 1

        if prev_pids is not None:
            kill_all_replicas(ssh_clients, prev_pids)

        print("\n🎉 All experiments complete!")
        print(f"📄 Results saved to: {RESULTS_CSV}")

        # ✅ Send success email with results table + CSV attachment
        summary_table = build_summary_table(rows)
        send_email(
            subject=f"✅ BFT-SMaRt Experiments Complete ({completed}/{total} runs)",
            body=f"""
            <h2>✅ All experiments finished successfully!</h2>
            <p><b>Total runs:</b> {total}<br>
               <b>Completed:</b> {completed}<br>
               <b>Results file:</b> experiments_result.csv (attached)</p>
            <h3>Results Summary</h3>
            {summary_table}
            """,
            attach_csv=True
        )

    except KeyboardInterrupt:
        interrupted = True
        print("\n⚠️  Interrupted! Cleaning up...")
        if prev_pids:
            kill_all_replicas(ssh_clients, prev_pids)

        # ✅ Send interruption email
        send_email(
            subject=f"⚠️  BFT-SMaRt Experiments Interrupted ({completed}/{total} runs)",
            body=f"""
            <h2>⚠️ Experiments were interrupted!</h2>
            <p><b>Completed before interrupt:</b> {completed}/{total} runs<br>
               <b>Tip:</b> Set <code>START_FROM_RUN = {completed + 1}</code> to resume.</p>
            """,
            attach_csv=True
        )

    finally:
        force_kill_all_nodes(ssh_clients)
        for client in ssh_clients.values():
            client.close()
        print("SSH connections closed.")
