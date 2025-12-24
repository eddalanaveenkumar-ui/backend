from fastapi import APIRouter
import pymongo
import logging
from typing import Optional

from .database import videos_collection

logger = logging.getLogger("uvicorn")
router = APIRouter()

def _format_video(video):
    return {
        "id": video.get("video_id"),
        "title": video.get("title"),
        "thumbnail": video.get("thumbnail_url"),
        "channel": video.get("channel_title"),
        "views": video.get("view_count", 0),
        "likes": video.get("like_count", 0),
        "published_at": video.get("published_at"),
        "is_short": video.get("is_short", False),
        "duration": video.get("duration", "PT0S")
    }

@router.get("/feed")
def get_feed(state: Optional[str] = None, language: Optional[str] = None, limit: int = 20):
    """
    Gets a personalized feed based on state and language.
    Falls back to global if no personalization is provided or if no results found.
    """
    logger.info(f"Feed request: state={state}, language={language}")
    
    videos = []
    
    # 1. Try Personalized Query
    if state or language:
        query = {}
        if state: query["state"] = state
        if language: query["language"] = language
        
        logger.info(f"Executing personalized query: {query}")
        videos = list(videos_collection.find(query).sort("viral_score", pymongo.DESCENDING).limit(limit))
        logger.info(f"Found {len(videos)} personalized videos")

    # 2. Fallback to Global if empty
    if not videos:
        logger.info("Personalized feed empty or not requested. Falling back to global feed.")
        videos = list(videos_collection.find({}).sort("viral_score", pymongo.DESCENDING).limit(limit))
        logger.info(f"Found {len(videos)} global videos by viral_score")

        # 3. Ultimate Fallback: If still no videos (e.g., no viral_score field), sort by date
        if not videos:
            logger.info("No videos found with viral_score. Falling back to sorting by published_at.")
            videos = list(videos_collection.find({}).sort("published_at", pymongo.DESCENDING).limit(limit))
            logger.info(f"Found {len(videos)} global videos by date")

    # Format for frontend
    formatted_videos = [_format_video(v) for v in videos]
    return formatted_videos
