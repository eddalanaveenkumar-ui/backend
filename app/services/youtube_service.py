import datetime
import requests
from ..database import api_key_usage_collection, videos_collection, channels_collection
from ..config import settings
import logging

logger = logging.getLogger("uvicorn")

class YouTubeService:
    def __init__(self):
        self.api_keys = [key.strip() for key in settings.YOUTUBE_API_KEYS if key.strip()]
        self.current_key_index = 0
        self._initialize_keys()

    def _initialize_keys(self):
        if not self.api_keys:
            logger.warning("⚠️ NO API KEYS FOUND! Please check your .env file.")
            return

        for key in self.api_keys:
            exists = api_key_usage_collection.find_one({"api_key": key})
            if not exists:
                api_key_usage_collection.insert_one({
                    "api_key": key,
                    "daily_quota_used": 0,
                    "is_active": True,
                    "last_used": datetime.datetime.utcnow()
                })
        logger.info(f"✅ Loaded and verified {len(self.api_keys)} API keys.")

    def get_next_active_key(self):
        """
        Finds the next available key using a round-robin strategy.
        It will try each key starting from the last used one.
        """
        if not self.api_keys:
            return None

        # Try each key once, starting from the current index
        for _ in range(len(self.api_keys)):
            key_to_check = self.api_keys[self.current_key_index]
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys) # Move to next key for next time

            usage = api_key_usage_collection.find_one({"api_key": key_to_check, "is_active": True})
            if not usage:
                continue

            # Reset quota if it's a new day
            now = datetime.datetime.utcnow()
            if usage.get("last_used") and usage["last_used"].date() < now.date():
                usage["daily_quota_used"] = 0
                api_key_usage_collection.update_one(
                    {"_id": usage["_id"]},
                    {"$set": {"daily_quota_used": 0, "last_used": now}}
                )

            if usage.get("daily_quota_used", 0) < 9500:
                return usage # Found a valid key

        return None # No keys have quota left

    def increment_quota(self, key_obj, cost=1):
        api_key_usage_collection.update_one(
            {"_id": key_obj["_id"]},
            {
                "$inc": {"daily_quota_used": cost},
                "$set": {"last_used": datetime.datetime.utcnow()}
            }
        )

    def fetch_videos(self, query, niche, state, language, max_results=50):
        key_usage = self.get_next_active_key()
        if not key_usage:
            logger.error("❌ All API keys have exhausted their quotas for today.")
            return []

        api_key = key_usage["api_key"]
        
        search_url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet", "q": f"{query} {niche} {language}", "type": "video",
            "maxResults": max_results, "order": "date", "regionCode": "IN",
            "publishedAfter": (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat("T") + "Z",
            "key": api_key
        }
        
        try:
            response = requests.get(search_url, params=params)
            
            if response.status_code == 403:
                logger.error(f"🚫 Quota exceeded for key: {api_key[:5]}... Marking as exhausted for today.")
                self.increment_quota(key_usage, cost=10000) # Mark as fully used
                return self.fetch_videos(query, niche, state, language, max_results) # Retry with the next key

            self.increment_quota(key_usage, cost=100)
            
            if response.status_code != 200:
                logger.error(f"YouTube API Error on Search: {response.text}")
                return []
                
            video_ids = [item["id"]["videoId"] for item in response.json().get("items", [])]
            if not video_ids: return []

            videos_url = "https://www.googleapis.com/youtube/v3/videos"
            v_params = {"part": "snippet,contentDetails,statistics", "id": ",".join(video_ids), "key": api_key}
            v_response = requests.get(videos_url, params=v_params)
            self.increment_quota(key_usage, cost=1)
            
            if v_response.status_code != 200:
                logger.error(f"YouTube API Error on Videos: {v_response.text}")
                return []
                
            for item in v_response.json().get("items", []):
                self._process_video_item(item, niche, state, language)
            
            logger.info(f"Successfully fetched {len(video_ids)} videos for '{niche} | {state} | {language}'")

        except Exception as e:
            logger.error(f"Exception in fetch_videos: {e}")
            return []

    def _process_video_item(self, item, niche, state, language):
        video_id = item["id"]
        snippet = item["snippet"]
        channel_id = snippet["channelId"]
        
        # Upsert channel
        channels_collection.update_one(
            {"channel_id": channel_id},
            {"$setOnInsert": {
                "channel_name": snippet["channelTitle"], "language": language,
                "primary_state": state, "subscriber_count": 0
            }},
            upsert=True
        )

        video_data = {
            "video_id": video_id, "title": snippet["title"], "description": snippet["description"],
            "channel_id": channel_id, "channel_title": snippet["channelTitle"],
            "niche": niche, "state": state, "language": language,
            "published_at": datetime.datetime.strptime(snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"),
            "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
            "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
            "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
            "thumbnail_url": snippet["thumbnails"]["high"]["url"],
            "is_short": "S" in item.get("contentDetails", {}).get("duration", "") and "M" not in item.get("contentDetails", {}).get("duration", ""),
            "duration": item.get("contentDetails", {}).get("duration", "PT0S"),
            "created_at": datetime.datetime.utcnow()
        }
        video_data["viral_score"] = self.calculate_viral_score(video_data)
        
        videos_collection.update_one({"video_id": video_id}, {"$set": video_data}, upsert=True)

    def calculate_viral_score(self, video_data):
        age_hours = max((datetime.datetime.utcnow() - video_data["published_at"]).total_seconds() / 3600, 0.1)
        views_per_hour = video_data.get("view_count", 0) / age_hours
        engagement_ratio = 0
        if video_data.get("view_count", 0) > 0:
            engagement_ratio = (video_data.get("like_count", 0) + (video_data.get("comment_count", 0) * 2)) / video_data["view_count"]
        
        score = views_per_hour * (1 + engagement_ratio * 10)
        if video_data.get("is_short", False): score *= 1.5
            
        state_lang_map = {"Maharashtra": "Marathi", "Tamil Nadu": "Tamil", "Andhra Pradesh": "Telugu", "Telangana": "Telugu", "Karnataka": "Kannada", "Kerala": "Malayalam", "West Bengal": "Bengali", "Gujarat": "Gujarati", "Punjab": "Punjabi"}
        if video_data.get("state") in state_lang_map and video_data.get("language") == state_lang_map[video_data["state"]]:
            score *= 1.2
            
        return score
