import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Antigravity\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    
    print(f"Dumping all keys from {db_path}...")
    
    cursor.execute("SELECT key FROM ItemTable;")
    rows = cursor.fetchall()
    
    for row in rows:
        print(row[0])
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
