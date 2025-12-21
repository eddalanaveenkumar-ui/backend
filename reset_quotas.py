from app.database import api_key_usage_collection
import datetime

def reset_quotas():
    print("Resetting API key quotas...")
    result = api_key_usage_collection.update_many(
        {},
        {"$set": {"daily_quota_used": 0, "last_used": datetime.datetime.utcnow()}}
    )
    print(f"Reset quotas for {result.modified_count} keys.")

if __name__ == "__main__":
    reset_quotas()
