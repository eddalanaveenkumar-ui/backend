from ..database import videos_collection, viral_index_collection
import pymongo

class ViralEngine:
    def __init__(self):
        pass

    def update_viral_indices(self):
        """
        Re-calculates and populates the ViralIndex collection for:
        - GLOBAL
        - STATE
        - LANGUAGE
        - STATE_LANGUAGE
        """
        # Clear old indices
        viral_index_collection.delete_many({})
        
        # 1. GLOBAL VIRAL
        global_videos = videos_collection.find().sort("viral_score", pymongo.DESCENDING).limit(100)
        for rank, vid in enumerate(global_videos, 1):
            self._add_index(vid, "GLOBAL", rank)

        # 2. STATE VIRAL
        states = videos_collection.distinct("state")
        for state in states:
            if not state: continue
            videos = videos_collection.find({"state": state}).sort("viral_score", pymongo.DESCENDING).limit(50)
            for rank, vid in enumerate(videos, 1):
                self._add_index(vid, "STATE", rank, state=state)

        # 3. LANGUAGE VIRAL
        languages = videos_collection.distinct("language")
        for lang in languages:
            if not lang: continue
            videos = videos_collection.find({"language": lang}).sort("viral_score", pymongo.DESCENDING).limit(50)
            for rank, vid in enumerate(videos, 1):
                self._add_index(vid, "LANGUAGE", rank, language=lang)
                
        # 4. STATE_LANGUAGE VIRAL
        # Get unique combinations via aggregation
        pipeline = [
            {"$group": {"_id": {"state": "$state", "language": "$language"}}}
        ]
        combos = videos_collection.aggregate(pipeline)
        
        for combo in combos:
            state = combo["_id"].get("state")
            lang = combo["_id"].get("language")
            if not state or not lang: continue
            
            videos = videos_collection.find({"state": state, "language": lang}).sort("viral_score", pymongo.DESCENDING).limit(50)
            for rank, vid in enumerate(videos, 1):
                self._add_index(vid, "STATE_LANGUAGE", rank, state=state, language=lang)

    def _add_index(self, video, v_type, rank, state=None, language=None):
        idx = {
            "video_id": video["video_id"],
            "viral_type": v_type,
            "score": video["viral_score"],
            "rank": rank,
            "state": state,
            "language": language
        }
        viral_index_collection.insert_one(idx)
