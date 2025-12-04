import os

# --- API KEYS ---
# Checks config.py first, then environment variables for safety
GEMINI_API_KEY = "YOUR_GEMINI_KEY_HERE" 

# --- FILES ---
INPUT_FILE = "Universal_Library_System_Export_Final.csv"
OUTPUT_FILE = "Enriched_Library_Master.csv"
PROGRESS_FILE = "processing_journal.json"
KEYS_FILE = "api_keys.txt"
LOG_DIR = "session_logs"

# --- PERFORMANCE TUNING ---
BATCH_SIZE = 30  # REDUCED to accommodate rich "Review & Rating" data
RATE_LIMIT_DELAY = 4.0
MAX_RETRIES = 5

# --- MODELS ---
PREFERRED_MODELS = [
    'models/gemini-2.0-flash',
    'models/gemini-2.0-flash-lite',
    'models/gemini-1.5-flash',
    'models/gemini-pro'
]