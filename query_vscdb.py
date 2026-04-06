import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Code\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print("DB not found")
    exit(1)

try:
    # Use uri=True and mode=ro to avoid locking issues with VS Code
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in state.vscdb:")
    for t in tables:
        print(f"- {t[0]}")
    
    # Look for Gemini-related keys in the ItemTable (common name)
    cursor.execute("SELECT key FROM ItemTable WHERE key LIKE '%gemini%' OR key LIKE '%duetAI%' LIMIT 50;")
    keys = cursor.fetchall()
    print("\nGemini-related keys in ItemTable:")
    for k in keys:
        print(f"- {k[0]}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
