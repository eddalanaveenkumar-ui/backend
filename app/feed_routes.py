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
    skip: int = 0
    is_short: Optional[bool] = None

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
    # ... (code is unchanged)
    try:
        state = request.state
        language = request.language
        limit = request.limit
        skip = request.skip
        is_short = request.is_short
        
        logger.info(f"Feed request: state={state}, language={language}, skip={skip}, is_short={is_short}")
        
        videos = []
        projection = {"_id": 0}
        
        def build_query(base_query):
            if is_short is not None:
                base_query["is_short"] = is_short
            return base_query

        def run_query(query):
            return list(videos_collection.find(query, projection).sort("viral_score", pymongo.DESCENDING).skip(skip).limit(limit))

        if state and language:
            videos = run_query(build_query({"state": state, "language": language}))
        
        if not videos and language:
            videos = run_query(build_query({"language": language}))

        if not videos and state:
            videos = run_query(build_query({"state": state}))

        if not videos:
            videos = run_query(build_query({}))

        if not videos:
            logger.info("No videos found with viral_score. Falling back to sorting by published_at.")
            fallback_query = build_query({})
            videos = list(videos_collection.find(fallback_query, projection).sort("published_at", pymongo.DESCENDING).skip(skip).limit(limit))

        logger.info(f"Returning {len(videos)} videos")
        
        formatted_videos = [_format_video(v) for v in videos]
        return formatted_videos

    except Exception as e:
        logger.error(f"Error in get_feed: {e}")
        traceback.print_exc()
        return []

@router.get("/video/{video_id}")
def get_video_details(video_id: str):
    """
    Gets details for a single video from the database.
    """
    logger.info(f"Fetching details for video_id: {video_id}")
    projection = {"_id": 0}
    video = videos_collection.find_one({"video_id": video_id}, projection)
    
    if not video:
        raise HTTPException(status_code=404, detail="Video not found in database")
        
    return _format_video(video)
