import pandas as pd
import json
import os
import shutil
import threading
import queue
from config import INPUT_FILE, PROGRESS_FILE, OUTPUT_FILE
from models import BookGroup, parse_library_date

class DataManager:
    def __init__(self):
        self.full_df = None
        self.unique_groups = {} 
        self.hash_map = {}      
        self.processed_hashes = set()
        self.results_cache = {} 
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
        
        print("Loading CSV Data...")
        self.full_df = pd.read_csv(INPUT_FILE, dtype=str).fillna('')
        
        print("Grouping duplicates to optimize AI usage...")
        
        for idx, row in self.full_df.iterrows():
            # 1. Identity
            t = row.get('full_title_245', '')
            a = row.get('author_main_100a', '')
            c = row.get('co_authors_700a', '')
            
            temp_group = BookGroup(t, a, c)
            h = temp_group.content_hash
            
            # 2. Map & Store
            if h not in self.hash_map:
                self.hash_map[h] = []
                self.unique_groups[h] = temp_group 
            
            self.hash_map[h].append(idx)
            
            # 3. Refine Context (Harvesting best metadata from duplicates)
            group = self.unique_groups[h]
            
            # Edition (Keep longest string)
            curr_ed = str(row.get('edition_250a', ''))
            if len(curr_ed) > len(group.best_edition):
                group.best_edition = curr_ed
                
            # Date (Keep oldest)
            dates = [
                parse_library_date(row.get('pub_year_008')),
                parse_library_date(row.get('bill_date_952_b')),
                parse_library_date(row.get('date_acquired_952d'))
            ]
            valid_dates = [d for d in dates if d]
            if valid_dates:
                local_min = min(valid_dates)
                if group.oldest_date is None or local_min < group.oldest_date:
                    group.oldest_date = local_min
            
            # --- NEW: Harvest Dewey & Subject ---
            # Dewey: Keep the longest one (usually most specific, e.g., 620.11 vs 620)
            curr_dewey = str(row.get('dewey_class_082a', '')).strip()
            if len(curr_dewey) > len(group.dewey):
                group.dewey = curr_dewey
            
            # Subject: Keep the longest one (most descriptive)
            curr_subj = str(row.get('subject_650a', '')).strip()
            if len(curr_subj) > len(group.subjects):
                group.subjects = curr_subj

        print(f"Optimization: Compressed {len(self.full_df)} rows into {len(self.unique_groups)} unique API calls.")

    def _load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                try: 
                    self.results_cache = json.load(f)
                    self.processed_hashes = set(self.results_cache.keys())
                except: pass

    def get_pending_batches(self, batch_size):
        pending_hashes = [h for h in self.unique_groups if h not in self.processed_hashes]
        batches = []
        current_batch = []
        for h in pending_hashes:
            current_batch.append(self.unique_groups[h])
            if len(current_batch) >= batch_size:
                batches.append(current_batch)
                current_batch = []
        if current_batch:
            batches.append(current_batch)
        return batches

    def submit_result(self, batch_result_dict):
        self.result_queue.put(batch_result_dict)

    def _writer_loop(self):
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                batch = self.result_queue.get(timeout=1)
                self.results_cache.update(batch)
                self.processed_hashes.update(batch.keys())
                
                temp = PROGRESS_FILE + ".tmp"
                with open(temp, 'w') as f: json.dump(self.results_cache, f, indent=2)
                shutil.move(temp, PROGRESS_FILE)
                self.result_queue.task_done()
            except queue.Empty: continue
            except Exception as e: print(f"WRITE ERROR: {e}")

    def stop(self):
        self.stop_event.set()
        self.writer_thread.join()

    def export_final_csv(self):
        print("Mapping enriched data back to original dataset...")
        enrichment_data = {}
        
        for content_hash, result in self.results_cache.items():
            if content_hash in self.hash_map:
                row_indices = self.hash_map[content_hash]
                flat_res = {
                    'ai_summary': result.get('description', ''),
                    'ai_tags': json.dumps(result.get('tags', [])),
                    'ai_critical_review': result.get('critical_review', ''),
                    'ai_score_relevance': result.get('scores', {}).get('relevance', 0),
                    'ai_score_readability': result.get('scores', {}).get('readability', 0),
                    'ai_score_depth': result.get('scores', {}).get('depth', 0),
                    'is_outdated': result.get('is_outdated', False)
                }
                for idx in row_indices:
                    enrichment_data[idx] = flat_res
        
        enrich_df = pd.DataFrame.from_dict(enrichment_data, orient='index')
        final_df = self.full_df.join(enrich_df)
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"Export complete: {OUTPUT_FILE}")