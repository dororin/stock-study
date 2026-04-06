import os
import datetime
import sys

sys.stdout.reconfigure(encoding='utf-8')

brain_dir = r"C:\Users\RYOTA\.gemini\antigravity\brain"
items = os.listdir(brain_dir)
history = []

for item in items:
    path = os.path.join(brain_dir, item)
    if os.path.isdir(path):
        ctime = os.path.getctime(path)
        dt = datetime.datetime.fromtimestamp(ctime)
        history.append((dt, item))

# Sort by time descending
history.sort(key=lambda x: x[0], reverse=True)

now = datetime.datetime.now()
print(f"Current Time: {now.strftime('%Y-%m-%d %H:%M')}")
print("-" * 50)
print("Status | Date | ID Prefix")
print("-" * 50)

for dt, item in history:
    # Anything within the last 5 days
    diff = now - dt
    status = "MISSING from UI" if diff.days < 5 else "VISIBLE in UI"
    # Overwrite status based on user's screenshot if we could, 
    # but for now let's just show the last 10.
    print(f"{status} | {dt.strftime('%Y-%m-%d %H:%M')} | {item[:8]}")
