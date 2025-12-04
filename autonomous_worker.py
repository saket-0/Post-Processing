import google.generativeai as genai
import time
import json
import logging
import random
from config import PREFERRED_MODELS, RATE_LIMIT_DELAY

def worker_task(batch, key_manager, data_manager):
    """
    1. Acquire a valid key.
    2. Process the batch.
    3. If 429, report failure and RETRY with a new key.
    4. If success, report success and submit data.
    """
    
    # Retry loop for the specific batch
    while True:
        key_obj = key_manager.get_available_key()
        
        if not key_obj:
            logging.warning("⚠️ No active keys available! Worker sleeping...")
            time.sleep(30)
            continue

        api_key = key_obj.key
        
        # Rate Limit Sleep (Per Key)
        time_since_use = time.time() - key_obj.last_used
        if time_since_use < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - time_since_use)

        try:
            # --- AI CALL ---
            genai.configure(api_key=api_key)
            
            # Construct Prompt
            # We explicitly tell the AI that the "Key" is immutable.
            batch_text = "\n".join([f"ID: '{fp}' -> Book: {fp.split('|')[0]} (Author: {fp.split('|')[1] if '|' in fp else 'Unknown'})" for fp in batch])
            
            prompt = f"""
            You are a Bibliographic Data Enrichment Engine for a Technical University Library.
            I will provide a list of {len(batch)} books with unique string IDs.
            
            YOUR TASK:
            For each book, analyze the Title and Author to infer its technical contents.
            
            OUTPUT REQUIREMENTS:
            Return a single, valid JSON object where:
            1. The dictionary KEYS are exactly the "ID" strings provided in the input (Do not modify them).
            2. The VALUES are objects containing:
               - "description": A dense, 2-3-sentence technical summary (approx 25-80 words). Focus on what the student will learn (e.g., "Covers thermodynamics, entropy, and heat transfer...").
               - "tags": A list of 5-8 specific domain keywords that will help us recognise the book's content, offerings, and topics to read easily (e.g., "Fluid Mechanics", "Navier-Stokes", "Aerodynamics").
            [Note: Avoid reusing the words already present in the title of the book, and author, because we are building a search efficient system, and they are already searchable.]
            
            INPUT DATA:
            {batch_text}
            
            STRICT JSON FORMAT EXAMPLE:
            {{
              "engineering drawing | bhatt...": {{
                "description": "A comprehensive guide to orthographic projections and isometric views...",
                "tags": ["Orthographic", "Isometric", "CAD", "Drafting"]
              }}
            }}
            """
            
            # Model Loop
            result = None
            for model_name in PREFERRED_MODELS:
                try:
                    model = genai.GenerativeModel(model_name)
                    resp = model.generate_content(prompt)
                    text = resp.text.replace("```json", "").replace("```", "").strip()
                    if "{" in text: text = text[text.find("{"):text.rfind("}")+1]
                    result = json.loads(text)
                    break # Success
                except Exception as e:
                    if "429" in str(e): raise e # Escalate to Key Manager
                    continue # Try next model
            
            if result:
                # Success!
                key_manager.report_success(api_key)
                
                # Format for saving
                clean_result = {}
                for fp in batch:
                    if fp in result:
                        data = result[fp]
                        data['cover_image'] = ""
                        clean_result[fp] = data
                    else:
                        clean_result[fp] = {"description": "", "tags": []}
                
                data_manager.submit_result(clean_result)
                return # Task Complete
            
            else:
                raise Exception("All models failed or returned invalid JSON")

        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Quota" in error_msg:
                key_manager.report_failure(api_key, "Quota Exceeded")
            elif "400" in error_msg:
                key_manager.report_failure(api_key, "Invalid Key")
            else:
                logging.error(f"Worker Error: {error_msg}")
                # Don't ban key for random network errors, just retry
            
            # Random sleep before retry to prevent thundering herd
            time.sleep(random.uniform(2, 5))