import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Antigravity\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    
    print(f"Listing all antigravity-related keys in {db_path}...")
    
    cursor.execute("SELECT key, value FROM ItemTable WHERE key LIKE '%antigravity%' OR key LIKE '%unifiedStateSync%';")
    rows = cursor.fetchall()
    
    for key, value in rows:
        val_str = str(value)
        if len(val_str) > 150:
            val_str = val_str[:150] + "..."
        print(f"Key: {key}\nValue: {val_str}\n")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
