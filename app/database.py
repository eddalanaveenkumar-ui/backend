from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from .config import settings

client = MongoClient(settings.MONGO_URI)
db = client[settings.MONGO_DB_NAME]

# --- Core Data Collections ---
videos_collection = db["videos"]
channels_collection = db["channels"]
viral_index_collection = db["viral_index"]
api_key_usage_collection = db["api_key_usage"]

# --- User Data Collections ---
users_collection = db["users"]
user_activity_collection = db["user_activity"]
user_follows_collection = db["user_follows"]


def create_indexes():
    # Videos indexes
    videos_collection.create_index("video_id", unique=True)
    videos_collection.create_index([("state", 1), ("language", 1)])
    videos_collection.create_index("viral_score")
    videos_collection.create_index([("state", 1)])
    videos_collection.create_index([("language", 1)])
    videos_collection.create_index([("published_at", DESCENDING)])
    
    # Channels indexes
    channels_collection.create_index("channel_id", unique=True)
    
    # Viral Index indexes
    viral_index_collection.create_index([("viral_type", 1), ("rank", 1)])
    viral_index_collection.create_index([("viral_type", 1), ("state", 1), ("language", 1)])
    
    # API Key Usage indexes
    api_key_usage_collection.create_index("api_key", unique=True)

    # User Data Indexes
    users_collection.create_index("uid", unique=True)
    users_collection.create_index("username", unique=True)
    
    # User Activity Indexes - Optimized for Feed Analysis
    user_activity_collection.create_index([("uid", 1), ("video_id", 1)], unique=True)
    user_activity_collection.create_index([("uid", 1), ("liked", 1)])
    user_activity_collection.create_index([("uid", 1), ("replay_count", -1)])
    user_activity_collection.create_index([("uid", 1), ("paused_at", 1)])

    user_follows_collection.create_index([("uid", 1), ("channel_id", 1)], unique=True)


# Initialize indexes on startup
create_indexes()
