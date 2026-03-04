#!/usr/bin/env python3
"""
BFT-SMaRt + YCSB Manager: Update system.config + workloads/workloada
Usage: python3 script.py 100 -1 16 0.5 0.5 0.1 0.2
   batchtimeout maxbatchsize numrepliers readprop updateprop hotspotdata hotspotopn
"""

import os
import sys
import argparse

def delete_file_in_script_dir(filename):
    """Remove filename from script's directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(script_dir, filename)
    try:
        os.remove(target)
        print(f"✅ Deleted: {target}")
        return True
    except FileNotFoundError:
        print(f"ℹ️  File not found (OK): {target}")
        return True
    except Exception as e:
        print(f"❌ Delete failed: {e}")
        return False

def update_system_config(updates):
    """Update system.config in script dir."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'system.config')
    try:
        with open(config_path, 'r') as f:
            lines = f.readlines()
        props = {}
        for i, line in enumerate(lines):
            stripped = line.strip()
            if '=' in stripped and not stripped.startswith('#'):
                key, value = stripped.split('=', 1)
                props[key.strip()] = i
        for key, new_val in updates.items():
            if key in props:
                old_i = props[key]
                lines[old_i] = f"{key} = {new_val}\n"
                print(f"✅ BFT {key} → {new_val}")
            else:
                lines.append(f"{key} = {new_val}\n")
                print(f"➕ BFT {key} = {new_val}")
        with open(config_path, 'w') as f:
            f.writelines(lines)
        print(f"✅ system.config → {config_path}")
        return True
    except Exception as e:
        print(f"❌ BFT config failed: {e}")
        return False

def update_ycsb_workloada(updates):
    """Update workloads/workloada in script dir."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    workload_dir = os.path.join(script_dir, 'workloads')
    workload_path = os.path.join(workload_dir, 'workloada')
    
    try:
        with open(workload_path, 'r') as f:
            lines = f.readlines()
        props = {}
        for i, line in enumerate(lines):
            stripped = line.strip()
            if '=' in stripped and not stripped.startswith('#'):
                key, value = stripped.split('=', 1)
                props[key.strip()] = i
        for key, new_val in updates.items():
            if key in props:
                old_i = props[key]
                lines[old_i] = f"{key}={new_val}\n"
                print(f"✅ YCSB {key} → {new_val}")
            else:
                lines.append(f"{key}={new_val}\n")
                print(f"➕ YCSB {key} = {new_val}")
        with open(workload_path, 'w') as f:
            f.writelines(lines)
        print(f"✅ workloada → {workload_path}")
        return True
    except Exception as e:
        print(f"❌ YCSB workload failed: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update BFT + YCSB configs")
    parser.add_argument("batchtimeout", type=int)
    parser.add_argument("maxbatchsize", type=int)
    parser.add_argument("numrepliers", type=int)
    parser.add_argument("readproportion", type=float)
    parser.add_argument("updateproportion", type=float)
    parser.add_argument("hotspotdatafraction", type=float)
    parser.add_argument("hotspotopnfraction", type=float)
    args = parser.parse_args()
    
    print("=== BFT-SMaRt + YCSB Config Update ===")
    print(f"BFT: batchtimeout={args.batchtimeout}, maxbatchsize={args.maxbatchsize}, numrepliers={args.numrepliers}")
    print(f"YCSB: read={args.readproportion}, update={args.updateproportion}, hotspot_data={args.hotspotdatafraction}, hotspot_opn={args.hotspotopnfraction}")
    
    # 1. Delete CurrentView
    if not delete_file_in_script_dir('currentView'):
        sys.exit(1)
    
    # 2. Update system.config
    bft_updates = {
        'system.totalordermulticast.batchtimeout': str(args.batchtimeout),
        'system.totalordermulticast.maxbatchsize': str(args.maxbatchsize),
        'system.numrepliers': str(args.numrepliers)
    }
    if not update_system_config(bft_updates):
        sys.exit(1)
    
    # 3. Update workloada
    ycsb_updates = {
        'readproportion': str(args.readproportion),
        'updateproportion': str(args.updateproportion),
        'hotspotdatafraction': str(args.hotspotdatafraction),
        'hotspotopnfraction': str(args.hotspotopnfraction)
    }
    if not update_ycsb_workloada(ycsb_updates):
        sys.exit(1)
    
    print("\n🎉 Both configs updated! Ready for benchmark.")
    print("Run: ./ycsb run mongodb -P workloads/workloada ...")
    sys.exit(0)
