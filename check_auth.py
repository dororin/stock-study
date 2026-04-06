import sqlite3
import os
import json

db_path = r"C:\Users\RYOTA\AppData\Roaming\Antigravity\User\globalStorage\state.vscdb"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    
    print(f"Analyzing Authentication in {db_path}...")
    
    # Check Auth Status
    cursor.execute("SELECT value FROM ItemTable WHERE key = 'antigravityAuthStatus';")
    row = cursor.fetchone()
    if row:
        auth_data = row[0]
        print(f"Auth Status: {auth_data}")
    else:
        print("antigravityAuthStatus not found.")
        
    # Check for anything else related to identity or accounts
    cursor.execute("SELECT key, value FROM ItemTable WHERE key LIKE '%account%' OR key LIKE '%user%' OR key LIKE '%profile%' LIMIT 20;")
    rows = cursor.fetchall()
    print("\nRelated Profile/Account keys:")
    for key, value in rows:
        print(f"Key: {key} -> {str(value)[:100]}")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
