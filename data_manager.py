import pandas as pd
import json
import os
import shutil
import threading
import queue
import time
from config import INPUT_FILE, PROGRESS_FILE, OUTPUT_FILE

class DataManager:
    def __init__(self):
        self.df = None
        self.unique_fingerprints = []
        self.processed_data = {}
        
        # Thread-Safe Queue for results
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        
    def start(self):
        self._load_data()
        self._load_progress()
        self.writer_thread.start()

    def _load_data(self):
        if not os.path.exists(INPUT_FILE):
            raise FileNotFoundError(f"Input file {INPUT_FILE} not found!")
        
        cols = ['full_title_245', 'author_main_100a']
        self.df = pd.read_csv(INPUT_FILE, usecols=lambda c: c in cols, dtype=str)
        
        self.df['fingerprint'] = (
            self.df['full_title_245'].fillna('').str.strip().str.lower() + " | " + 
            self.df['author_main_100a'].fillna('').str.strip().str.lower()
        )
        self.unique_fingerprints = [f for f in self.df['fingerprint'].unique() if len(f) > 5]

    def _load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                try:
                    self.processed_data = json.load(f)
                except json.JSONDecodeError:
                    pass

    def get_pending_batches(self, batch_size):
        all_pending = [fp for fp in self.unique_fingerprints if fp not in self.processed_data]
        return [all_pending[i:i + batch_size] for i in range(0, len(all_pending), batch_size)]

    def submit_result(self, batch_result_dict):
        """Workers call this to save data."""
        self.result_queue.put(batch_result_dict)

    def _writer_loop(self):
        """Single thread dedicated to saving data. No race conditions possible."""
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                # Block for 1 sec, then check stop event
                batch = self.result_queue.get(timeout=1)
                
                # Update memory
                self.processed_data.update(batch)
                
                # Atomic Write to Disk
                temp_file = PROGRESS_FILE + ".tmp"
                with open(temp_file, 'w') as f:
                    json.dump(self.processed_data, f, indent=2)
                shutil.move(temp_file, PROGRESS_FILE)
                
                self.result_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"CRITICAL WRITE ERROR: {e}")

    def stop(self):
        self.stop_event.set()
        self.writer_thread.join()

    def export_final_csv(self):
        # (Same export logic as previous versions)
        full_df = pd.read_csv(INPUT_FILE, dtype=str)
        full_df['fingerprint'] = (
            full_df['full_title_245'].fillna('').str.strip().str.lower() + " | " + 
            full_df['author_main_100a'].fillna('').str.strip().str.lower()
        )
        
        enrichment_list = []
        for fp, data in self.processed_data.items():
            enrichment_list.append({
                'fingerprint': fp,
                'ai_description': data.get('description', ''),
                'ai_tags': json.dumps(data.get('tags', [])),
                'cover_image_url': data.get('cover_image', '')
            })
            
        enrich_df = pd.DataFrame(enrichment_list)
        final_df = pd.merge(full_df, enrich_df, on='fingerprint', how='left')
        final_df.drop(columns=['fingerprint'], inplace=True)
        final_df.to_csv(OUTPUT_FILE, index=False)