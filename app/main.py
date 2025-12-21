from fastapi import FastAPI, BackgroundTasks
from apscheduler.schedulers.background import BackgroundScheduler
import logging

from .database import viral_index_collection, videos_collection
from .services.youtube_service import YouTubeService
from .services.viral_engine import ViralEngine
from .config import settings
from .constants import NICHES, STATES, LANGUAGES
from .user_routes import router as user_router # Import the user router

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn")

app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION)

# Include the user router
app.include_router(user_router, prefix="/api", tags=["User"])

# Scheduler for background tasks
scheduler = BackgroundScheduler()

def comprehensive_fetch_job():
    """
    Background job to fetch videos for ALL defined niches, states, and languages.
    """
    logger.info("🚀 Starting comprehensive fetch job for all categories...")
    try:
        yt_service = YouTubeService()
        viral_engine = ViralEngine()
        
        # Loop through every combination
        for niche in NICHES:
            for state in STATES:
                for lang in LANGUAGES:
                    # Construct a query relevant to the niche + state + language
                    query = f"{niche} {state} {lang}"
                    logger.info(f"--> Fetching videos for: {niche} | {state} | {lang}")
                    yt_service.fetch_videos(query=query, niche=niche, state=state, language=lang, max_results=50)
        
        logger.info("🧠 Updating all viral indices...")
        viral_engine.update_viral_indices()
        logger.info("✅ Comprehensive fetch job completed successfully.")
    except Exception as e:
        logger.error(f"❌ Job failed with exception: {e}", exc_info=True)

# Start scheduler
@app.on_event("startup")
def start_scheduler():
    # Run every 60 minutes for a comprehensive run
    scheduler.add_job(comprehensive_fetch_job, 'interval', minutes=60, id="comprehensive_fetch")
    logger.info("Scheduler started. First comprehensive run will be in 60 minutes.")

@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()

# --- APIs ---

@app.get("/admin/trigger-fetch")
def trigger_fetch_manual(background_tasks: BackgroundTasks):
    """Manually triggers the comprehensive data collection job in the background."""
    logger.info("Manual fetch triggered via API. Adding to background tasks.")
    background_tasks.add_task(comprehensive_fetch_job)
    return {"status": "Comprehensive fetch job started in the background. Check server logs for progress."}

@app.get("/feed/global")
def get_global_viral(limit: int = 20):
    return _get_feed("GLOBAL", limit=limit)

@app.get("/feed/state/{state}")
def get_state_viral(state: str, limit: int = 20):
    return _get_feed("STATE", state=state, limit=limit)

@app.get("/feed/language/{language}")
def get_language_viral(language: str, limit: int = 20):
    return _get_feed("LANGUAGE", language=language, limit=limit)

@app.get("/feed/state-language/{state}/{language}")
def get_state_language_viral(state: str, language: str, limit: int = 20):
    return _get_feed("STATE_LANGUAGE", state=state, language=language, limit=limit)

def _get_feed(viral_type, state=None, language=None, limit=20):
    query = {"viral_type": viral_type}
    if state: query["state"] = state
    if language: query["language"] = language
        
    cursor = viral_index_collection.find(query).sort("rank", 1).limit(limit)
    
    video_ids = [idx["video_id"] for idx in cursor]
    videos = {v["video_id"]: v for v in videos_collection.find({"video_id": {"$in": video_ids}})}
    
    feed = []
    for idx in cursor:
        video = videos.get(idx["video_id"])
        if video:
            feed.append({
                "rank": idx["rank"], "score": idx["score"], "viral_type": idx["viral_type"],
                "video": _format_video(video)
            })
    return feed

def _format_video(video):
    return {
        "id": video["video_id"], "title": video["title"], "thumbnail": video["thumbnail_url"],
        "channel": video["channel_title"], "views": video["view_count"], "likes": video["like_count"],
        "published_at": video["published_at"], "is_short": video["is_short"]
    }
