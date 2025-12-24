from fastapi import APIRouter, HTTPException, Body
import pymongo
import logging
from typing import Optional, List
import traceback
from pydantic import BaseModel

from .database import videos_collection

logger = logging.getLogger("uvicorn")
router = APIRouter()

class FeedRequest(BaseModel):
    state: Optional[str] = None
    language: Optional[str] = None
    limit: int = 20

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

@router.post("/feed")
def get_feed(request: FeedRequest):
    """
    Gets a personalized feed with multi-level fallback.
    1. State + Language
    2. Language Only
    3. State Only
    4. Global Viral
    5. Most Recent (Failsafe)
    """
    try:
        state = request.state
        language = request.language
        limit = request.limit
        
        logger.info(f"Feed request: state={state}, language={language}")
        
        videos = []
        projection = {"_id": 0} # Exclude ObjectId
        
        # 1. Try State + Language
        if state and language:
            query = {"state": state, "language": language}
            logger.info(f"Trying State + Language: {query}")
            videos = list(videos_collection.find(query, projection).sort("viral_score", pymongo.DESCENDING).limit(limit))
        
        # 2. Try Language Only
        if not videos and language:
            query = {"language": language}
            logger.info(f"Trying Language Only: {query}")
            videos = list(videos_collection.find(query, projection).sort("viral_score", pymongo.DESCENDING).limit(limit))

        # 3. Try State Only
        if not videos and state:
            query = {"state": state}
            logger.info(f"Trying State Only: {query}")
            videos = list(videos_collection.find(query, projection).sort("viral_score", pymongo.DESCENDING).limit(limit))

        # 4. Fallback to Global Viral
        if not videos:
            logger.info("Falling back to Global Viral")
            videos = list(videos_collection.find({}, projection).sort("viral_score", pymongo.DESCENDING).limit(limit))

        # 5. Ultimate Fallback: Most Recent
        if not videos:
            logger.info("Falling back to Most Recent")
            videos = list(videos_collection.find({}, projection).sort("published_at", pymongo.DESCENDING).limit(limit))

        logger.info(f"Returning {len(videos)} videos")
        
        # Format for frontend
        formatted_videos = [_format_video(v) for v in videos]
        return formatted_videos

    except Exception as e:
        logger.error(f"Error in get_feed: {e}")
        traceback.print_exc()
        # Return a 200 OK with empty list instead of 500 to prevent app crash
        return []
