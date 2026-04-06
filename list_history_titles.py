import os
import re

brain_dir = r"C:\Users\RYOTA\.gemini\antigravity\brain"
items = os.listdir(brain_dir)
history_list = []

for item in items:
    path = os.path.join(brain_dir, item)
    if os.path.isdir(path):
        title = "Unknown Topic"
        # Try to find a title in implementation_plan.md or task.md
        plan_path = os.path.join(path, "implementation_plan.md")
        task_path = os.path.join(path, "task.md")
        
        target = None
        if os.path.exists(plan_path):
            target = plan_path
        elif os.path.exists(task_path):
            target = task_path
            
        if target:
            with open(target, 'r', encoding='utf-8') as f:
                content = f.read(500) # Read first 500 chars
                # Look for # Title or [Goal Description] or Task: Name
                match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
                if match:
                    title = match.group(1).strip()
                else:
                    match = re.search(r'Task:\s+(.+)$', content, re.MULTILINE)
                    if match:
                        title = match.group(1).strip()

        ctime = os.path.getctime(path)
        history_list.append({
            "id": item,
            "title": title,
            "created": ctime
        })

# Sort by time
history_list.sort(key=lambda x: x["created"], reverse=True)

import datetime
for h in history_list:
    dt = datetime.datetime.fromtimestamp(h["created"]).strftime('%Y-%m-%d %H:%M')
    print(f"{dt} | {h['id'][:8]} | {h['title']}")
