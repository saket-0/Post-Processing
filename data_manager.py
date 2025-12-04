import pandas as pd
import json
import os
import shutil
import threading
import queue
from config import INPUT_FILE, PROGRESS_FILE, OUTPUT_FILE

class DataManager:
    def __init__(self):
        self.df = None
        self.unique_fingerprints = []
        self.processed_data = {}
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
        
        # --- CRITICAL UPDATE: Loading Year and Edition ---
        cols = ['full_title_245', 'author_main_100a', 'edition_250a', 'pub_year_008']
        self.df = pd.read_csv(INPUT_FILE, usecols=lambda c: c in cols, dtype=str)
        
        # Helper to clean strings
        def clean(val): return str(val).strip().replace('nan', '')

        # We bake the metadata DIRECTLY into the fingerprint string so the worker sees it
        # Format: "Title | Author | Edition | Year"
        self.df['fingerprint'] = (
            self.df['full_title_245'].apply(clean) + " | " + 
            self.df['author_main_100a'].apply(clean) + " | " + 
            self.df['edition_250a'].apply(clean) + " | " + 
            self.df['pub_year_008'].apply(clean)
        )
        
        # Deduplicate (ignore identical books)
        self.unique_fingerprints = [f for f in self.df['fingerprint'].unique() if len(f) > 10]

    def _load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                try: self.processed_data = json.load(f)
                except: pass

    def get_pending_batches(self, batch_size):
        # Only process what hasn't been done
        pending = [fp for fp in self.unique_fingerprints if fp not in self.processed_data]
        return [pending[i:i + batch_size] for i in range(0, len(pending), batch_size)]

    def submit_result(self, batch_result_dict):
        self.result_queue.put(batch_result_dict)

    def _writer_loop(self):
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                batch = self.result_queue.get(timeout=1)
                self.processed_data.update(batch)
                
                # Atomic Save
                temp = PROGRESS_FILE + ".tmp"
                with open(temp, 'w') as f: json.dump(self.processed_data, f, indent=2)
                shutil.move(temp, PROGRESS_FILE)
                self.result_queue.task_done()
            except queue.Empty: continue
            except Exception as e: print(f"WRITE ERROR: {e}")

    def stop(self):
        self.stop_event.set()
        self.writer_thread.join()

    def export_final_csv(self):
        # Reload full CSV to merge
        full_df = pd.read_csv(INPUT_FILE, dtype=str)
        
        def clean(val): return str(val).strip().replace('nan', '')
        
        full_df['fingerprint'] = (
            full_df['full_title_245'].apply(clean) + " | " + 
            full_df['author_main_100a'].apply(clean) + " | " + 
            full_df['edition_250a'].apply(clean) + " | " + 
            full_df['pub_year_008'].apply(clean)
        )
        
        enrich_list = []
        for fp, data in self.processed_data.items():
            enrich_list.append({
                'fingerprint': fp,
                'ai_summary': data.get('summary', ''),
                'ai_tags': json.dumps(data.get('tags', [])),
                'score_relevance': data.get('scores', {}).get('relevance', 0),
                'score_readability': data.get('scores', {}).get('readability', 0),
                'score_depth': data.get('scores', {}).get('depth', 0),
                'is_outdated': data.get('is_outdated', False)
            })
            
        enrich_df = pd.DataFrame(enrich_list)
        final_df = pd.merge(full_df, enrich_df, on='fingerprint', how='left')
        final_df.drop(columns=['fingerprint'], inplace=True)
        final_df.to_csv(OUTPUT_FILE, index=False)