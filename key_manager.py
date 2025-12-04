import time
import os
import logging
from dataclasses import dataclass
from enum import Enum
import threading

class KeyStatus(Enum):
    ACTIVE = "🟢 Active"
    COOLDOWN = "🟡 Cooldown"
    DEAD = "🔴 Dead"

@dataclass
class APIKey:
    key: str
    status: KeyStatus = KeyStatus.ACTIVE
    last_used: float = 0
    cooldown_until: float = 0
    total_requests: int = 0
    errors: int = 0

class KeyManager:
    def __init__(self, key_file):
        self.key_file = key_file
        self.keys = {} # Dict[str, APIKey]
        self.lock = threading.Lock()
        self.last_file_check = 0
        self._load_keys()

    def _load_keys(self):
        """Reads the file and adds new keys. Never removes keys (to keep stats)."""
        if not os.path.exists(self.key_file):
            return

        current_mtime = os.path.getmtime(self.key_file)
        if current_mtime <= self.last_file_check:
            return

        with open(self.key_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]

        with self.lock:
            for k in lines:
                if k not in self.keys:
                    self.keys[k] = APIKey(key=k)
                    logging.info(f"🔑 New Key Detected: {k[:6]}...")
            
        self.last_file_check = current_mtime

    def get_available_key(self):
        """Returns a valid key that is not in cooldown."""
        self._load_keys() # Check for new keys first
        
        with self.lock:
            now = time.time()
            best_key = None
            
            for k, key_obj in self.keys.items():
                if key_obj.status == KeyStatus.DEAD:
                    continue
                
                if key_obj.status == KeyStatus.COOLDOWN:
                    if now > key_obj.cooldown_until:
                        key_obj.status = KeyStatus.ACTIVE
                        logging.info(f"♻️ Key Revived: {k[:6]}...")
                    else:
                        continue

                # If active, return it
                if key_obj.status == KeyStatus.ACTIVE:
                    return key_obj

        return None

    def report_success(self, key_str):
        with self.lock:
            if key_str in self.keys:
                self.keys[key_str].last_used = time.time()
                self.keys[key_str].total_requests += 1
                self.keys[key_str].errors = 0 # Reset consecutive errors

    def report_failure(self, key_str, error_type="generic"):
        with self.lock:
            if key_str not in self.keys: return
            
            key_obj = self.keys[key_str]
            key_obj.errors += 1
            
            if "429" in str(error_type) or "Quota" in str(error_type):
                key_obj.status = KeyStatus.COOLDOWN
                # 24 hours cooldown for daily limit
                key_obj.cooldown_until = time.time() + (60 * 60 * 24) 
                logging.warning(f"📉 Key {key_str[:6]} hit QUOTA. Cooling down for 24h.")
            
            elif "400" in str(error_type) or "API_KEY_INVALID" in str(error_type):
                key_obj.status = KeyStatus.DEAD
                logging.error(f"💀 Key {key_str[:6]} is INVALID. Marked Dead.")

    def get_stats(self):
        with self.lock:
            return [
                {
                    "key": k[:6] + "...", 
                    "status": v.status.value, 
                    "reqs": v.total_requests,
                    "errors": v.errors  # <--- THIS WAS MISSING
                } 
                for k, v in self.keys.items()
            ]