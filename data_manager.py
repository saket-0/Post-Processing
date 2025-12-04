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
        self.unique_groups = {} # Map[content_hash, BookGroup]
        self.hash_map = {}      # Map[content_hash, List[original_indices]]
        
        self.processed_hashes = set()
        self.results_cache = {} # Map[content_hash, ResultDict]
        
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
        
        # --- ROBUST GROUPING STRATEGY ---
        print("Grouping duplicates to optimize AI usage...")
        
        for idx, row in self.full_df.iterrows():
            # 1. Create a temporary group to generate the hash
            t = row.get('full_title_245', '')
            a = row.get('author_main_100a', '')
            c = row.get('co_authors_700a', '')
            
            # Using our model to generate consistent hash
            temp_group = BookGroup(t, a, c)
            h = temp_group.content_hash
            
            # 2. Map this row index to the hash
            if h not in self.hash_map:
                self.hash_map[h] = []
                self.unique_groups[h] = temp_group # Store the first one as representative
            
            self.hash_map[h].append(idx)
            
            # 3. Refine the Representative (Optional but good for data quality)
            # We try to find the best edition or oldest date among the duplicates
            # to give the AI the most complete picture.
            existing_group = self.unique_groups[h]
            
            # Check for better edition info
            curr_ed = str(row.get('edition_250a', ''))
            if len(curr_ed) > len(existing_group.best_edition):
                existing_group.best_edition = curr_ed
                
            # Check for older date (often implies original publication)
            # We check pub_year, bill_date, acquired_date
            dates = [
                parse_library_date(row.get('pub_year_008')),
                parse_library_date(row.get('bill_date_952_b')),
                parse_library_date(row.get('date_acquired_952d'))
            ]
            valid_dates = [d for d in dates if d]
            if valid_dates:
                local_min = min(valid_dates)
                if existing_group.oldest_date is None or local_min < existing_group.oldest_date:
                    existing_group.oldest_date = local_min

        print(f"Optimization: Compressed {len(self.full_df)} rows into {len(self.unique_groups)} unique API calls.")

    def _load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                try: 
                    self.results_cache = json.load(f)
                    self.processed_hashes = set(self.results_cache.keys())
                except: pass

    def get_pending_batches(self, batch_size):
        # Return list of BookGroup objects for hashes that aren't processed
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
        """
        batch_result_dict: { 'hash_id': {data...}, ... }
        """
        self.result_queue.put(batch_result_dict)

    def _writer_loop(self):
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                batch = self.result_queue.get(timeout=1)
                
                # Update cache
                self.results_cache.update(batch)
                self.processed_hashes.update(batch.keys())
                
                # Persist to disk (Resume capability)
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
        
        # Create list to hold new columns
        enrichment_data = {} # Map[row_index, dict_of_new_values]
        
        # Iterate through our results and broadcast to original rows
        for content_hash, result in self.results_cache.items():
            if content_hash in self.hash_map:
                row_indices = self.hash_map[content_hash]
                
                # Flatten the AI result
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
        
        # Convert dictionary to DataFrame
        enrich_df = pd.DataFrame.from_dict(enrichment_data, orient='index')
        
        # Join with original dataframe
        # We ensure indices align
        final_df = self.full_df.join(enrich_df)
        
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"Export complete: {OUTPUT_FILE}")