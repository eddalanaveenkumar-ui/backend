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
    skip: int = 0 # Add skip for pagination

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
    Gets a personalized feed with multi-level fallback and pagination.
    """
    try:
        state = request.state
        language = request.language
        limit = request.limit
        skip = request.skip
        
        logger.info(f"Feed request: state={state}, language={language}, skip={skip}")
        
        videos = []
        projection = {"_id": 0}
        
        # Define a helper to run queries
        def run_query(query):
            return list(videos_collection.find(query, projection).sort("viral_score", pymongo.DESCENDING).skip(skip).limit(limit))

        # 1. Try State + Language
        if state and language:
            videos = run_query({"state": state, "language": language})
        
        # 2. Try Language Only
        if not videos and language:
            videos = run_query({"language": language})

        # 3. Try State Only
        if not videos and state:
            videos = run_query({"state": state})

        # 4. Fallback to Global Viral
        if not videos:
            videos = run_query({})

        # 5. Ultimate Fallback: Most Recent
        if not videos:
            logger.info("No videos found with viral_score. Falling back to sorting by published_at.")
            videos = list(videos_collection.find({}, projection).sort("published_at", pymongo.DESCENDING).skip(skip).limit(limit))

        logger.info(f"Returning {len(videos)} videos")
        
        formatted_videos = [_format_video(v) for v in videos]
        return formatted_videos

    except Exception as e:
        logger.error(f"Error in get_feed: {e}")
        traceback.print_exc()
        return []
