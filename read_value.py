import sqlite3
import os

db_path = r"C:\Users\RYOTA\AppData\Roaming\Code\User\globalStorage\state.vscdb"

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM ItemTable WHERE key = 'google.geminicodeassist';")
    row = cursor.fetchone()
    if row:
        print(row[0])
    else:
        print("Key not found")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
