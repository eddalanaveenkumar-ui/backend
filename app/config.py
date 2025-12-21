import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    PROJECT_NAME: str = "Triangle Backend"
    PROJECT_VERSION: str = "1.0.0"
    
    # Database
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "triangle_db")
    
    # YouTube API Keys (Comma separated in .env)
    # Default keys are placeholders. You must provide valid keys in .env or here.
    YOUTUBE_API_KEYS: list = os.getenv("YOUTUBE_API_KEYS", "").split(",")

settings = Settings()
