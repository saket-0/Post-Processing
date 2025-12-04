import os

# --- FILES ---
INPUT_FILE = "Universal_Library_System_Export_Final.csv"
OUTPUT_FILE = "Enriched_Library_Master.csv"
PROGRESS_FILE = "processing_journal.json"
KEYS_FILE = "api_keys.txt"
LOG_DIR = "session_logs"

# --- TUNING ---
BATCH_SIZE = 75 
RATE_LIMIT_DELAY = 4.0 # Seconds between requests PER KEY
MAX_RETRIES = 5

# --- MODELS ---
PREFERRED_MODELS = [
    'models/gemini-2.0-flash',
    'models/gemini-2.0-flash-lite',
    'models/gemini-1.5-flash',
    'models/gemini-pro'
]