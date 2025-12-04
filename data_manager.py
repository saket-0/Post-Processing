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
        
        # --- LOADING ALL NECESSARY COLUMNS ---
        cols = [
            'full_title_245', 
            'author_main_100a', 
            'co_authors_700a',      # <-- NEW: Added for deduplication
            'edition_250a',         
            'pub_year_008',         
            'date_entered_008',     
            'bill_date_952_b',      
            'date_acquired_952d',   
            'date_last_seen_952r'   
        ]
        
        # Load with string types to preserve format
        self.df = pd.read_csv(INPUT_FILE, usecols=lambda c: c in cols, dtype=str)
        
        # Helper to clean text
        def clean(val): return str(val).strip().replace('nan', '')

        # --- DEDUPLICATION LOGIC (4-part key: Title, Author, Co-Author, Edition) ---
        self.df['dedup_key'] = (
            self.df['full_title_245'].apply(clean) + "|||" + 
            self.df['author_main_100a'].apply(clean) + "|||" + 
            self.df['co_authors_700a'].apply(clean) + "|||" + # <-- NEW: Included Co-Authors
            self.df['edition_250a'].apply(clean)
        )
        
        # Filter the DataFrame to keep only one record per unique book
        self.df.drop_duplicates(subset=['dedup_key'], keep='first', inplace=True)
        self.df.drop(columns=['dedup_key'], inplace=True) 

        # --- RICH FINGERPRINT CREATION (8-part key for worker logic and persistence) ---
        # The structure used by the worker for splitting/provenance must be maintained
        self.df['fingerprint'] = (
            self.df['full_title_245'].apply(clean) + "|||" + 
            self.df['author_main_100a'].apply(clean) + "|||" + 
            self.df['edition_250a'].apply(clean) + "|||" + 
            self.df['pub_year_008'].apply(clean) + "|||" +
            self.df['date_entered_008'].apply(clean) + "|||" +
            self.df['bill_date_952_b'].apply(clean) + "|||" +
            self.df['date_acquired_952d'].apply(clean) + "|||" +
            self.df['date_last_seen_952r'].apply(clean)
        )
        
        # Extract unique fingerprints from the pre-deduplicated DataFrame
        self.unique_fingerprints = [f for f in self.df['fingerprint'].unique() if len(f) > 10]

    def _load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                try: self.processed_data = json.load(f)
                except: pass

    def get_pending_batches(self, batch_size):
        pending = [fp for fp in self.unique_fingerprints if fp not in self.processed_data]
        return [pending[i:i + batch_size] for i in range(0, len(pending), batch_size)]

    def submit_result(self, batch_result_dict):
        self.result_queue.put(batch_result_dict)

    def _writer_loop(self):
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                batch = self.result_queue.get(timeout=1)
                self.processed_data.update(batch)
                
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
        full_df = pd.read_csv(INPUT_FILE, dtype=str)
        
        # Re-create the 8-part fingerprint logic to match keys
        def clean(val): return str(val).strip().replace('nan', '')
        
        full_df['fingerprint'] = (
            full_df['full_title_245'].apply(clean) + "|||" + 
            full_df['author_main_100a'].apply(clean) + "|||" + 
            full_df['edition_250a'].apply(clean) + "|||" + 
            full_df['pub_year_008'].apply(clean) + "|||" +
            full_df['date_entered_008'].apply(clean) + "|||" +
            full_df['bill_date_952_b'].apply(clean) + "|||" +
            full_df['date_acquired_952d'].apply(clean) + "|||" +
            full_df['date_last_seen_952r'].apply(clean)
        )
        
        enrich_list = []
        for fp, data in self.processed_data.items():
            enrich_list.append({
                'fingerprint': fp,
                'ai_summary': data.get('summary', ''),
                'ai_tags': json.dumps(data.get('tags', [])),
                'ai_critical_review': data.get('critical_review', ''),
                'score_relevance': data.get('scores', {}).get('relevance', 0),
                'score_readability': data.get('scores', {}).get('readability', 0),
                'score_depth': data.get('scores', {}).get('depth', 0),
                'is_outdated': data.get('is_outdated', False)
            })
            
        enrich_df = pd.DataFrame(enrich_list)
        final_df = pd.merge(full_df, enrich_df, on='fingerprint', how='left')
        final_df.drop(columns=['fingerprint'], inplace=True)
        final_df.to_csv(OUTPUT_FILE, index=False)