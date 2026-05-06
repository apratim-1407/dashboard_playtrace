import os
import json
import pymongo
from datetime import datetime

# Connect to MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/playtrace")
client = pymongo.MongoClient(MONGO_URI)
try:
    db = client.get_database()
except Exception:
    db = client["playtrace"]

# Export events
events = list(db.events.find({}, {"_id": 0}))
# Export sessions
sessions = list(db.sessions.find({}, {"_id": 0}))

# Create data directory if it doesn't exist
os.makedirs("data", exist_ok=True)

from bson import json_util

# Write to files
with open("data/events.json", "w") as f:
    f.write(json_util.dumps(events))

with open("data/sessions.json", "w") as f:
    f.write(json_util.dumps(sessions))

# Write meta.json
with open("data/meta.json", "w") as f:
    json.dump({"lastUpdated": datetime.utcnow().isoformat() + "Z"}, f)

print(f"Exported {len(events)} events and {len(sessions)} sessions.")
