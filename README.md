# Triangle Backend System (MongoDB Version)

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables**
   Create a `.env` file in `backend_python/`:
   ```
   MONGO_URI=mongodb://localhost:27017
   MONGO_DB_NAME=triangle_db
   YOUTUBE_API_KEYS=key1,key2,key3,key4
   ```

3. **Run Server**
   ```bash
   uvicorn app.main:app --reload
   ```

## Architecture

- **FastAPI**: High performance web framework.
- **MongoDB**: NoSQL document storage for flexibility and speed.
- **Viral Engine**: Calculates scores based on view velocity and engagement.
- **Scheduler**: Runs every 30 mins to fetch new videos and update viral ranks.

## API Endpoints

- `GET /feed/global`
- `GET /feed/state/{state}`
- `GET /feed/language/{language}`
- `GET /feed/state-language/{state}/{language}`
