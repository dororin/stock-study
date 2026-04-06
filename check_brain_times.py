import os
import datetime

# Using absolute path for Windows
brain_dir = r"C:\Users\RYOTA\.gemini\antigravity\brain"
if not os.path.exists(brain_dir):
    print(f"Error: {brain_dir} not found")
    exit(1)

items = os.listdir(brain_dir)
results = []
for item in items:
    path = os.path.join(brain_dir, item)
    if os.path.isdir(path):
        # On Windows, os.path.getctime is creation time, os.path.getmtime is modification time
        try:
            ctime = os.path.getctime(path)
            results.append({
                "name": item,
                "created": datetime.datetime.fromtimestamp(ctime).isoformat()
            })
        except Exception as e:
            print(f"Error reading {item}: {e}")

# Sort by creation time
results.sort(key=lambda x: x["created"])

for r in results:
    print(f"{r['created']} | {r['name']}")
