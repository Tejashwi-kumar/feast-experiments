import os
import sys
import shutil
import argparse
import boto3
import subprocess
import importlib

def clean_room_execution():
    parser = argparse.ArgumentParser()
    parser.add_argument('--artifact_path', type=str, required=True)
    args, unknown = parser.parse_known_args()

    # 1. Setup Isolated Paths
    # We use /tmp because EMR Serverless allows write access there
    base_dir = "/tmp/ml_runtime"
    artifact_dir = os.path.join(base_dir, "extracted_artifact")
    
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(artifact_dir)

    # 2. Download Artifact
    local_zip = os.path.join(base_dir, "build.zip")
    s3 = boto3.client('s3')
    bucket = args.artifact_path.split('/')[2]
    key = '/'.join(args.artifact_path.split('/')[3:])
    s3.download_file(bucket, key, local_zip)

    # 3. Unzip to our isolated directory
    subprocess.check_call(["unzip", "-q", local_zip, "-d", artifact_dir])

    # 4. HANDLE EDGE CASE: Solving Module Collision
    # ---------------------------------------------------------
    # A. Remove the current working directory (Citta code) from sys.path
    # This prevents 'import utils' from picking up Citta's utils.py
    cwd = os.getcwd()
    if cwd in sys.path:
        sys.path.remove(cwd)
    if '' in sys.path:
        sys.path.remove('')

    # B. Explicitly purge modules already loaded by Citta that might conflict
    # We look for any module that was loaded from the Citta directory
    to_purge = [name for name, mod in sys.modules.items() 
                if hasattr(mod, '__file__') and mod.__file__ and cwd in mod.__file__]
    for mod_name in to_purge:
        del sys.modules[mod_name]

    # C. Set our Artifact as the Absolute Priority
    sys.path.insert(0, artifact_dir)
    # ---------------------------------------------------------

    # 5. Execute with high-priority pathing
    try:
        importlib.invalidate_caches()
        # Ensure your TeamCity build wraps everything in a unique package name
        # e.g., 'protected_core'
        from protected_core.main import execute_ml_pipeline
        
        print("--- [CLEAN ROOM] Starting isolated execution ---")
        execute_ml_pipeline(unknown_args=unknown)
        
    except Exception as e:
        print(f"--- [CRITICAL] Execution Failure: {e} ---")
        sys.exit(1)

if __name__ == "__main__":
    clean_room_execution()
