import time
import threading
import uuid
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import config
from data_manager import DataManager
from key_manager import KeyManager
from ui_manager import Dashboard  # <--- Using our new UI
from autonomous_worker import worker_task
from rich.live import Live

# Logging setup
if not os.path.exists(config.LOG_DIR): os.makedirs(config.LOG_DIR)
SESSION_ID = f"AUTO-{str(uuid.uuid4())[:6]}"
LOG_FILE = f"{config.LOG_DIR}/{SESSION_ID}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s | %(message)s')

def main():
    # 1. Start Managers
    data_mgr = DataManager()
    data_mgr.start()
    
    key_mgr = KeyManager(config.KEYS_FILE)
    
    # 2. Prepare Data
    pending_batches = data_mgr.get_pending_batches(config.BATCH_SIZE)
    total_items = len(data_mgr.unique_fingerprints)
    current_completed = len(data_mgr.processed_data)
    
    # 3. Initialize Dashboard
    dashboard = Dashboard(total_items)
    dashboard.update_progress(current_completed)
    dashboard.log(f"Session Started: {SESSION_ID}", "cyan")
    dashboard.log(f"Loaded {len(pending_batches)} batches", "green")

    # 4. Start Worker Swarm
    MAX_WORKERS = 12 # Higher than key count to allow for waiting/retrying
    
    with Live(refresh_per_second=4, screen=True) as live: # screen=True makes it FULL SCREEN
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for batch in pending_batches:
                future = executor.submit(worker_task, batch, key_mgr, data_mgr)
                futures.append(future)
                
                # Update UI periodically during submission
                if len(futures) % 10 == 0:
                    live.update(dashboard.get_layout(key_mgr.get_stats(), active_workers_count=len(futures)))

            dashboard.log("All batches submitted. Waiting...", "blue")
            
            # 5. Monitoring Loop
            while any(not f.done() for f in futures):
                # Fetch fresh stats
                current_completed = len(data_mgr.processed_data)
                active_count = sum(1 for f in futures if not f.done())
                
                # Update Dashboard Data
                dashboard.update_progress(current_completed)
                
                # Check for dead keys to log
                for k in key_mgr.get_stats():
                    if "Dead" in k['status']:
                        dashboard.log(f"ALERT: Key {k['key']} died!", "red")

                # Refresh Screen
                live.update(dashboard.get_layout(key_mgr.get_stats(), active_workers_count=active_count))
                time.sleep(0.5)

    # 6. Shutdown
    print("Stopping writers...")
    data_mgr.stop()
    print("Exporting CSV...")
    data_mgr.export_final_csv()
    print("✅ MISSION COMPLETE.")

if __name__ == "__main__":
    main()