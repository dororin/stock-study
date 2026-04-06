import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Antigravity\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    
    # Check telemetry.machineId
    cursor.execute("SELECT value FROM ItemTable WHERE key = 'telemetry.machineId';")
    row = cursor.fetchone()
    if row:
        print(f"DB machineId: {row[0]}")
    else:
        print("telemetry.machineId not found in DB.")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
