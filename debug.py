import google.generativeai as genai
import config  # <--- IMPORTING FROM YOUR CONFIG FILE NOW
import sys

# Check python version for that specific error you saw
if sys.version_info < (3, 10):
    print("⚠️  Warning: You are running Python 3.9. If this crashes, run:")
    print("   pip install importlib-metadata")

try:
    # Use the key you already saved in config.py
    API_KEY = config.GEMINI_API_KEY
    
    # Visual check to ensure it's not the placeholder
    if "YOUR_" in API_KEY or "HERE" in API_KEY:
        print("❌ ERROR: config.py still has the placeholder text!")
        print("   Please open config.py and paste your actual 'AIza...' key.")
        sys.exit(1)

    genai.configure(api_key=API_KEY)
    print(f"🔍 Testing API Key from config.py: {API_KEY[:5]}...{API_KEY[-5:]}")

    print("\nAttempting to list available models...")
    models = list(genai.list_models())
    
    print("✅ SUCCESS! Your API Key works. Available models:")
    for m in models:
        if 'generateContent' in m.supported_generation_methods:
            print(f"   - {m.name}")

except AttributeError:
    print("\n❌ PYTHON VERSION ERROR: Your Python 3.9 is missing a library.")
    print("👉 Run this command in terminal: pip install importlib-metadata")
except Exception as e:
    print(f"\n❌ API ERROR: {e}")