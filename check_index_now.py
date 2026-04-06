import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Antigravity\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    
    # Check if current conversation ID is in trajectorySummaries
    # f8483554-5621-4b81-8e92-6670875da122
    cursor.execute("SELECT key, value FROM ItemTable WHERE key = 'antigravityUnifiedStateSync.trajectorySummaries';")
    row = cursor.fetchone()
    
    if row:
        value = str(row[1])
        print(f"trajectorySummaries length: {len(value)}")
        if 'f8483554' in value:
            print("Current conversation (f8483554) is in the index!")
        else:
            print("Current conversation (f8483554) is NOT in the index.")
            
        # Check for other recent IDs from history_report.md
        recent_ids = ['4a9c982e', '94872e82', '50b4f542', 'b60a7e2d']
        for rid in recent_ids:
            if rid in value:
                print(f"ID {rid} is in the index.")
            else:
                print(f"ID {rid} is NOT in the index.")
    else:
        print("trajectorySummaries not found.")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
