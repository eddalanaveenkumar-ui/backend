from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import logging

from .database import videos_collection
from .services.youtube_service import YouTubeService
from .services.viral_engine import ViralEngine
from .config import settings
from .constants import NICHES, STATES, LANGUAGES
from .user_routes import router as user_router
from .feed_routes import router as feed_router # Import the new feed router

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn")

app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION)

# --- CORS Configuration ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Routers ---
app.include_router(user_router, prefix="/api", tags=["User"])
app.include_router(feed_router, prefix="/api", tags=["Feed"]) # Add the feed router with /api prefix

# --- Background Scheduler ---
scheduler = BackgroundScheduler()

def comprehensive_fetch_job():
    """
    Background job to fetch videos for ALL defined niches, states, and languages.
    """
    logger.info("🚀 Starting comprehensive fetch job for all categories...")
    try:
        yt_service = YouTubeService()
        viral_engine = ViralEngine()
        
        for niche in NICHES:
            for state in STATES:
                for lang in LANGUAGES:
                    query = f"{niche} {state} {lang}"
                    logger.info(f"--> Fetching videos for: {niche} | {state} | {lang}")
                    yt_service.fetch_videos(query=query, niche=niche, state=state, language=lang, max_results=50)
        
        logger.info("🧠 Updating all viral indices...")
        viral_engine.update_viral_indices()
        logger.info("✅ Comprehensive fetch job completed successfully.")
    except Exception as e:
        logger.error(f"❌ Job failed with exception: {e}", exc_info=True)

# --- App Lifecycle Events ---
@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(comprehensive_fetch_job, 'interval', minutes=60, id="comprehensive_fetch")
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started.")
    
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

# --- Root and Admin Endpoints ---
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
