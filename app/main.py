from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import logging

from .database import viral_index_collection, videos_collection
from .services.youtube_service import YouTubeService
from .services.viral_engine import ViralEngine
from .config import settings
from .constants import NICHES, STATES, LANGUAGES
from .user_routes import router as user_router

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn")

app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION)

# --- CORS Configuration (CRITICAL FOR MOBILE/WEB APPS) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Allows all headers
)

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
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started.")
    
    # --- DB Connection Check ---
    try:
        count = videos_collection.count_documents({})
        logger.info(f"✅ DATABASE CHECK: Found {count} videos in 'videos' collection.")
        if count == 0:
            logger.warning("⚠️ DATABASE IS EMPTY! Please run the fetch job.")
    except Exception as e:
        logger.error(f"❌ DATABASE CONNECTION ERROR: {e}")

@app.on_event("shutdown")
def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shut down.")

# --- APIs ---

@app.get("/")
def read_root():
    """Health check endpoint."""
    return {"status": "Triangle Backend is running", "version": settings.PROJECT_VERSION}

@app.get("/admin/trigger-fetch")
def trigger_fetch_manual(background_tasks: BackgroundTasks):
    """Manually triggers the comprehensive data collection job in the background."""
    logger.info("Manual fetch triggered via API. Adding to background tasks.")
    background_tasks.add_task(comprehensive_fetch_job)
    return {"status": "Comprehensive fetch job started in the background. Check server logs for progress."}

# --- Updated Feed Logic ---
@app.get("/feed")
def get_feed(state: str = None, language: str = None, limit: int = 20):
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
        videos = list(videos_collection.find(query).sort("viral_score", -1).limit(limit))
        logger.info(f"Found {len(videos)} personalized videos")

    # 2. Fallback to Global if empty
    if not videos:
        logger.info("Personalized feed empty or not requested. Falling back to global feed.")
        videos = list(videos_collection.find({}).sort("viral_score", -1).limit(limit))
        logger.info(f"Found {len(videos)} global videos")

    # Format for frontend
    formatted_videos = [_format_video(v) for v in videos]
    return formatted_videos

# Keep these for backward compatibility if needed, but /feed is the main one now
@app.get("/feed/global")
def get_global_viral(limit: int = 20):
    return get_feed(limit=limit)

@app.get("/feed/state/{state}")
def get_state_viral(state: str, limit: int = 20):
    return get_feed(state=state, limit=limit)

@app.get("/feed/language/{language}")
def get_language_viral(language: str, limit: int = 20):
    return get_feed(language=language, limit=limit)

@app.get("/feed/state-language/{state}/{language}")
def get_state_language_viral(state: str, language: str, limit: int = 20):
    return get_feed(state=state, language=language, limit=limit)

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
        "duration": video.get("duration", "PT0S") # Ensure duration is passed if available
    }
