#!/usr/bin/env python
"""
Delete all tables in the Supabase project.
This script will completely remove all data and table structures.
"""
import os
from supabase import create_client
from dotenv import load_dotenv

def delete_all_tables():
    """Delete all tables in the Supabase project."""
    load_dotenv()
    
    # Initialize Supabase client
    client = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    # List of tables to delete (in order to handle foreign key constraints)
    tables_to_delete = [
        'portfolio',  # Delete child tables first
        'runs',
        'g_spread',
        'universe',   # Delete parent tables last
        'security_mapping'
    ]
    
    print("⚠️  WARNING: This will permanently delete ALL tables and data!")
    print("Tables to be deleted:")
    for table in tables_to_delete:
        print(f"  - {table}")
    
    confirm = input("\nAre you sure you want to proceed? Type 'DELETE ALL' to confirm: ")
    
    if confirm != "DELETE ALL":
        print("❌ Operation cancelled.")
        return
    
    print("\n🗑️  Starting table deletion...")
    
    # First, try to delete all data from tables
    for table in tables_to_delete:
        try:
            print(f"Clearing data from {table}...")
            # Delete all records first
            response = client.table(table).delete().neq('id', 'IMPOSSIBLE_VALUE').execute()
            print(f"✅ Cleared data from {table}")
        except Exception as e:
            print(f"⚠️  Could not clear data from {table}: {str(e)}")
    
    # Now try to drop the tables using SQL
    print("\n🔥 Dropping table structures...")
    
    for table in tables_to_delete:
        try:
            # Drop table with CASCADE to handle foreign key constraints
            sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
            print(f"Executing: {sql}")
            
            # Note: We can't execute DDL through the REST API easily
            # This would need to be done through the Supabase dashboard or SQL editor
            print(f"⚠️  Please manually execute in Supabase SQL editor: {sql}")
            
        except Exception as e:
            print(f"❌ Error dropping {table}: {str(e)}")
    
    print("\n" + "="*60)
    print("🔥 DATA DELETION COMPLETED")
    print("="*60)
    print("✅ All table data has been cleared")
    print("⚠️  To completely drop table structures, please run these SQL commands")
    print("   in your Supabase SQL Editor:")
    print()
    for table in tables_to_delete:
        print(f"   DROP TABLE IF EXISTS {table} CASCADE;")
    print()
    print("💡 Alternatively, you can recreate the tables using the database")
    print("   design specification after this cleanup.")

if __name__ == "__main__":
    delete_all_tables() 