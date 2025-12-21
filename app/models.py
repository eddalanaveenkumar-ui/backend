from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class VideoModel(BaseModel):
    video_id: str
    title: str
    description: str
    channel_id: str
    channel_title: str
    niche: str
    state: str
    language: str
    published_at: datetime
    duration: int
    view_count: int
    like_count: int
    comment_count: int
    tags: Optional[List[str]] = []
    thumbnail_url: str
    is_short: bool
    viral_score: float = 0.0
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ChannelModel(BaseModel):
    channel_id: str
    channel_name: str
    language: str
    primary_state: str
    subscriber_count: int = 0

class ViralIndexModel(BaseModel):
    video_id: str
    viral_type: str  # GLOBAL, STATE, LANGUAGE, STATE_LANGUAGE
    score: float
    rank: int
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    state: Optional[str] = None
    language: Optional[str] = None

class ApiKeyUsageModel(BaseModel):
    api_key: str
    daily_quota_used: int = 0
    last_used: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
