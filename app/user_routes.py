from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

from .database import users_collection, user_activity_collection, user_follows_collection
from .firebase_config import verify_token

router = APIRouter()

# --- Pydantic Models for User Data ---
class UserProfile(BaseModel):
    state: str
    language: str

class UserActivity(BaseModel):
    video_id: str
    event_type: str  # 'WATCH', 'LIKE', 'UNLIKE', 'PAUSE', 'REPLAY'
    duration_watched: int = 0  # Seconds added to total watch time
    paused_at: Optional[float] = None # Timestamp in video where paused

class UserFollow(BaseModel):
    channel_id: str

# --- Helper for Authentication ---
def get_current_user(id_token: str = Body(..., embed=True)):
    uid = verify_token(id_token)
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return uid

# --- User API Endpoints ---

@router.post("/user/profile")
def update_user_profile(profile: UserProfile, uid: str = Depends(get_current_user)):
    """
    Creates or updates a user's profile with their state and language.
    """
    users_collection.update_one(
        {"uid": uid},
        {"$set": {"state": profile.state, "language": profile.language, "last_updated": datetime.utcnow()}},
        upsert=True
    )
    return {"status": "Profile updated successfully"}

@router.post("/user/activity")
def log_user_activity(activity: UserActivity, uid: str = Depends(get_current_user)):
    """
    Logs detailed user activity for analysis.
    Updates a single document per user-video pair.
    """
    update_data = {"$set": {"last_updated": datetime.utcnow()}}
    inc_data = {}

    if activity.event_type == 'WATCH':
        inc_data["duration_watched"] = activity.duration_watched
    
    elif activity.event_type == 'LIKE':
        update_data["$set"]["liked"] = True
        
    elif activity.event_type == 'UNLIKE':
        update_data["$set"]["liked"] = False

    elif activity.event_type == 'PAUSE':
        if activity.paused_at is not None:
            update_data["$set"]["paused_at"] = activity.paused_at
            update_data["$set"]["is_paused"] = True

    elif activity.event_type == 'REPLAY':
        inc_data["replay_count"] = 1
        update_data["$set"]["is_paused"] = False # If replaying, they are not paused anymore

    # Construct the final MongoDB update query
    mongo_update = update_data
    if inc_data:
        mongo_update["$inc"] = inc_data

    user_activity_collection.update_one(
        {"uid": uid, "video_id": activity.video_id},
        mongo_update,
        upsert=True
    )
    return {"status": "Activity logged"}

@router.post("/user/follow")
def follow_channel(follow: UserFollow, uid: str = Depends(get_current_user)):
    user_follows_collection.update_one(
        {"uid": uid, "channel_id": follow.channel_id},
        {"$set": {"followed_at": datetime.utcnow()}},
        upsert=True
    )
    return {"status": "Channel followed"}

@router.post("/user/unfollow")
def unfollow_channel(follow: UserFollow, uid: str = Depends(get_current_user)):
    user_follows_collection.delete_one({"uid": uid, "channel_id": follow.channel_id})
    return {"status": "Channel unfollowed"}

# --- Analysis Endpoint (For Feed Engine) ---
@router.get("/user/analysis")
def get_user_preferences(uid: str = Depends(get_current_user)):
    """
    Returns aggregated data to feed the recommendation engine.
    """
    # 1. Get Liked Videos
    liked_videos = list(user_activity_collection.find(
        {"uid": uid, "liked": True},
        {"video_id": 1, "_id": 0}
    ))
    
    # 2. Get Most Replayed Videos (Top 10)
    replayed_videos = list(user_activity_collection.find(
        {"uid": uid, "replay_count": {"$gt": 0}},
        {"video_id": 1, "replay_count": 1, "_id": 0}
    ).sort("replay_count", -1).limit(10))
    
    # 3. Get Currently Paused Videos (Resume list)
    paused_videos = list(user_activity_collection.find(
        {"uid": uid, "is_paused": True},
        {"video_id": 1, "paused_at": 1, "_id": 0}
    ).sort("last_updated", -1).limit(5))
    
    # 4. Get Followed Channels
    followed_channels = list(user_follows_collection.find(
        {"uid": uid},
        {"channel_id": 1, "_id": 0}
    ))

    return {
        "liked_video_ids": [v["video_id"] for v in liked_videos],
        "top_replayed": replayed_videos,
        "resume_watching": paused_videos,
        "followed_channel_ids": [c["channel_id"] for c in followed_channels]
    }
