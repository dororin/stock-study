import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Antigravity\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    
    print(f"Checking {db_path}...")
    
    # 履歴に関連しそうなエントリを検索
    patterns = ['%antigravity%', '%gemini%', '%chat%', '%history%', '%conversations%', '%brain%']
    for pattern in patterns:
        cursor.execute("SELECT key, value FROM ItemTable WHERE key LIKE ?;", (pattern,))
        rows = cursor.fetchall()
        print(f"\n--- Pattern: {pattern} ({len(rows)} entries) ---")
        for key, value in rows[:20]: # Show only first 20 for brevity
            val_str = str(value)
            if len(val_str) > 100:
                val_str = val_str[:100] + "..."
            print(f"Key: {key}\nValue: {val_str}\n")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
