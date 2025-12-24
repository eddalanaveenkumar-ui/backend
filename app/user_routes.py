from fastapi import APIRouter, Depends, HTTPException, Header, Body, Query
from pydantic import BaseModel
from typing import Optional, Annotated, List
from datetime import datetime

from .database import users_collection, user_activity_collection, user_follows_collection
from .firebase_config import verify_token

router = APIRouter()

# --- Pydantic Models ---
class UserProfile(BaseModel):
    state: Optional[str] = None
    language: Optional[str] = None
    photo_url: Optional[str] = None
    bio: Optional[str] = None
    display_name: Optional[str] = None

class UserRegistration(BaseModel):
    username: str
    email: str
    display_name: str

class UserActivity(BaseModel):
    video_id: str
    event_type: str
    duration_watched: int = 0
    paused_at: Optional[float] = None

class UserFollow(BaseModel):
    channel_id: str

class UsernameLookup(BaseModel):
    username: str

# --- Reusable Authentication Dependency ---
def get_current_user(authorization: Annotated[str | None, Header()] = None):
    if authorization is None:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    parts = authorization.split()
    if len(parts) != 2 or parts[0] != "Bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization scheme")
    
    id_token = parts[1]
    uid = verify_token(id_token)
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return uid

# --- User API Endpoints ---

@router.post("/user/register")
def register_user(data: UserRegistration, uid: str = Depends(get_current_user)):
    """
    Registers a new user with username, email, and display name.
    Checks if username is already taken.
    """
    # Check if username exists (case insensitive)
    existing_user = users_collection.find_one({"username": {"$regex": f"^{data.username}$", "$options": "i"}})
    if existing_user and existing_user.get("uid") != uid:
        raise HTTPException(status_code=400, detail="User ID already taken")

    users_collection.update_one(
        {"uid": uid},
        {"$set": {
            "username": data.username,
            "email": data.email,
            "display_name": data.display_name,
            "created_at": datetime.utcnow(),
            "last_updated": datetime.utcnow()
        }},
        upsert=True
    )
    return {"status": "User registered successfully"}

@router.post("/user/lookup")
def lookup_email_by_username(data: UsernameLookup):
    """
    Looks up an email address by username (User ID).
    Used for login when user enters a username instead of email.
    """
    user = users_collection.find_one({"username": {"$regex": f"^{data.username}$", "$options": "i"}})
    if user and "email" in user:
        return {"email": user["email"]}
    raise HTTPException(status_code=404, detail="User ID not found")

@router.get("/user/search")
def search_users(q: str = Query(..., min_length=1)):
    """
    Searches for users by username (User ID).
    Returns a list of matching users with their basic info.
    """
    users = list(users_collection.find(
        {"username": {"$regex": q, "$options": "i"}},
        {"username": 1, "photo_url": 1, "bio": 1, "_id": 0}
    ).limit(20))
    return users

@router.post("/user/profile")
def update_user_profile(profile: UserProfile, uid: str = Depends(get_current_user)):
    """Creates or updates a user's profile."""
    update_data = {"last_updated": datetime.utcnow()}
    if profile.state: update_data["state"] = profile.state
    if profile.language: update_data["language"] = profile.language
    if profile.photo_url: update_data["photo_url"] = profile.photo_url
    if profile.bio is not None: update_data["bio"] = profile.bio
    if profile.display_name: update_data["display_name"] = profile.display_name

    users_collection.update_one(
        {"uid": uid},
        {"$set": update_data},
        upsert=True
    )
    return {"status": "Profile updated successfully"}

@router.get("/user/profile")
def get_user_profile(uid: str = Depends(get_current_user)):
    """Retrieves a user's profile."""
    user_profile = users_collection.find_one({"uid": uid}, {"_id": 0})
    if user_profile:
        return {
            "username": user_profile.get("username"),
            "email": user_profile.get("email"),
            "display_name": user_profile.get("display_name"),
            "state": user_profile.get("state"),
            "language": user_profile.get("language"),
            "photo_url": user_profile.get("photo_url"),
            "bio": user_profile.get("bio")
        }
    raise HTTPException(status_code=404, detail="User profile not found")

# ... (rest of the file is unchanged) ...
