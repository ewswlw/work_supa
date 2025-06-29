#!/usr/bin/env python
"""Clear all data from Supabase tables."""
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

client = create_client(
    os.getenv('SUPABASE_URL'), 
    os.getenv('SUPABASE_SERVICE_ROLE_KEY')
)

tables = ['runs', 'portfolio', 'universe']  # Order matters due to foreign keys

print("Clearing tables...")
for table in tables:
    try:
        # Delete all records
        response = client.table(table).delete().neq('cusip', 'IMPOSSIBLE_VALUE_THAT_DOESNT_EXIST').execute()
        print(f"✅ Cleared {table} table")
    except Exception as e:
        print(f"❌ Error clearing {table}: {str(e)}")

print("\nDone!") 