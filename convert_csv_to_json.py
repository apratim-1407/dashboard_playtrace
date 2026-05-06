import csv
import json
from datetime import datetime

# Parse events
events = []
with open("playtrace.events.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Construct tags array
        tags = []
        if row.get("tags[0]"): tags.append(row["tags[0]"])
        if row.get("tags[1]"): tags.append(row["tags[1]"])
        
        event = {
            "_id": row.get("_id"),
            "sessionId": row.get("sessionId"),
            "stepId": row.get("stepId"),
            "decision": row.get("decision"),
            "tags": tags,
            "timestamp": row.get("timestamp"),
            "scenarioText": row.get("scenarioText"),
            "decisionText": row.get("decisionText"),
            "hesitationMs": float(row["hesitationMs"]) if row.get("hesitationMs") else None,
            "decisionTimeMs": float(row["decisionTimeMs"]) if row.get("decisionTimeMs") else None,
        }
        events.append(event)

with open("data/events.json", "w") as f:
    json.dump(events, f)

# Parse sessions
sessions = []
with open("playtrace_sessions_complete.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        session = {
            "_id": row.get("_id"),
            "sessionId": row.get("sessionId"),
            "startedAt": row.get("startedAt"),
            "completedAt": row.get("completedAt") if row.get("completedAt") else None,
            "metrics": {
                "avgDecisionTime": float(row["metrics.avgDecisionTime"]) if row.get("metrics.avgDecisionTime") else None,
                "avgHesitation": float(row["metrics.avgHesitation"]) if row.get("metrics.avgHesitation") else None,
                "riskScore": float(row["metrics.riskScore"]) if row.get("metrics.riskScore") else None,
                "consistencyScore": float(row["metrics.consistencyScore"]) if row.get("metrics.consistencyScore") else None,
                "variance": float(row["metrics.variance"]) if row.get("metrics.variance") else None,
                "pressureSensitivity": float(row["metrics.pressureSensitivity"]) if row.get("metrics.pressureSensitivity") else None,
            },
            "summary": {
                "highestRiskDecision": row.get("summary.highestRiskDecision"),
                "longestPause": row.get("summary.longestPause"),
                "biggestBehaviorShift": row.get("summary.biggestBehaviorShift"),
            }
        }
        sessions.append(session)

with open("data/sessions.json", "w") as f:
    json.dump(sessions, f)

# also update meta.json
with open("data/meta.json", "w") as f:
    json.dump({"lastUpdated": datetime.utcnow().isoformat() + "Z"}, f)

print(f"Conversion complete. Events: {len(events)}, Sessions: {len(sessions)}")
