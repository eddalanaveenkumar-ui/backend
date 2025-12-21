import firebase_admin
from firebase_admin import credentials, auth
import os
import json

# Path to the user data folder
USER_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "user_data")
KEY_FILE_PATH = os.path.join(USER_DATA_DIR, "serviceAccountKey.json")

# Cloud Deployment Strategy:
# 1. Try to load from local file in 'user_data/serviceAccountKey.json' (Development)
# 2. If not found, try to load from Environment Variable 'FIREBASE_CREDENTIALS' (Production/Cloud)

try:
    if os.path.exists(KEY_FILE_PATH):
        # Local Development
        cred = credentials.Certificate(KEY_FILE_PATH)
        print(f"✅ Loaded Firebase credentials from: {KEY_FILE_PATH}")
    else:
        # Cloud Deployment (Render/Railway)
        firebase_creds_json = os.getenv("FIREBASE_CREDENTIALS")
        if firebase_creds_json:
            creds_dict = json.loads(firebase_creds_json)
            cred = credentials.Certificate(creds_dict)
            print("✅ Loaded Firebase credentials from Environment Variable.")
        else:
            # Fallback check for the old location just in case
            old_path = "serviceAccountKey.json"
            if os.path.exists(old_path):
                 cred = credentials.Certificate(old_path)
                 print(f"⚠️ Loaded credentials from root folder. Please move to {USER_DATA_DIR}")
            else:
                raise FileNotFoundError(f"No serviceAccountKey.json found in {USER_DATA_DIR} and FIREBASE_CREDENTIALS env var not set.")

    firebase_admin.initialize_app(cred)
    print("✅ Firebase Admin SDK initialized successfully.")

except Exception as e:
    print(f"🔥 Firebase Admin SDK Error: {e}")
    print(f"👉 Please ensure 'serviceAccountKey.json' is inside the '{USER_DATA_DIR}' folder.")

def verify_token(id_token: str):
    """Verifies the Firebase ID token and returns the user's UID."""
    try:
        decoded_token = auth.verify_id_token(id_token)
        return decoded_token['uid']
    except Exception as e:
        return None
