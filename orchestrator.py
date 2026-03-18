import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv

# 1. Load environment and set initial defaults
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "db1db2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")

# Export to environ early
os.environ["DB_HOST"] = DB_HOST
os.environ["DB_PORT"] = DB_PORT
os.environ["DB_NAME"] = DB_NAME
os.environ["DB_USER"] = DB_USER
os.environ["DB_PASSWORD"] = DB_PASSWORD

# Now add paths and import
sys.path.append(os.path.join(os.path.dirname(__file__), "ADB1"))
sys.path.append(os.path.join(os.path.dirname(__file__), "ADB2"))

from ADB1.run import run_adb1
from ADB2.master import run_adb2

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def get_available_cities():
    """Fetch union of cities from ADB1 and ADB2 source tables."""
    global engine, DATABASE_URL
    try:
        with engine.connect() as conn:
            # We'll take unique cities from ADB2 as the primary reference
            res = conn.execute(text("SELECT DISTINCT city FROM property_transaction_db2 WHERE city IS NOT NULL")).fetchall()
            cities = sorted([r[0] for r in res])
            return cities
    except Exception as e:
        if "password authentication failed" in str(e).lower():
            print(f"\n❌ AUTHENTICATION FAILED for user '{DB_USER}' at {DB_HOST}.")
            print(f"   The current password '****' is incorrect.")
            new_pass = input(f"   Enter correct password for '{DB_USER}': ").strip()
            if new_pass:
                os.environ["DB_PASSWORD"] = new_pass
                DATABASE_URL = f"postgresql://{DB_USER}:{new_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
                engine = create_engine(DATABASE_URL)
                return get_available_cities()
        else:
            print(f"\n⚠ Could not fetch cities: {e}")
        return []

def check_source_tables_exist():
    """Verify if source tables for ADB1 and ADB2 exist in the DB."""
    required = [
        "ADB1_project_merged", "ADB2_project_wise_summary",
        "ADB1_location_merged", "ADB2_location_overview_combined",
        "ADB1_city_merged", "ADB2_city_overview_combined"
    ]
    try:
        from sqlalchemy import inspect
        inspector = inspect(engine)
        existing = inspector.get_table_names()
        missing = [t for t in required if t not in existing]
        return len(missing) == 0, missing
    except Exception as e:
        print(f"  ⚠ Error checking tables: {e}")
        return False, required
def merge_and_save_grand():
    """Reads ADB1 and ADB2 results from DB, merges them, and saves the grand result."""
    print("\n" + "="*50)
    print("  MERGING RESULTS (ADB1 + ADB2)")
    print("="*50)
    
    def id_based_outer_merge(adb1_df, adb2_df, id_col, label):
        """
        Merges two DataFrames on a single ID column using an outer join.
        - All rows are kept (unmatched rows appear as separate rows).
        - A 'source' column is added: 'Both', 'ADB1 Only', or 'ADB2 Only'.
        - Duplicate name columns (e.g. city_x / city_y) are consolidated into one.
        - IDs are cast to string for robust matching.
        """
        # Cast ID to string for reliable matching
        adb1_df = adb1_df.copy()
        adb2_df = adb2_df.copy()
        adb1_df[id_col] = adb1_df[id_col].astype(str).str.strip()
        adb2_df[id_col] = adb2_df[id_col].astype(str).str.strip()

        # Outer merge on ID column only
        merged = adb1_df.merge(adb2_df, on=id_col, how='outer', suffixes=('_ADB1', '_ADB2'))

        # Determine source for each row
        adb1_ids = set(adb1_df[id_col])
        adb2_ids = set(adb2_df[id_col])
        matched   = adb1_ids & adb2_ids
        adb1_only = adb1_ids - adb2_ids
        adb2_only = adb2_ids - adb1_ids

        def _source(val):
            if val in matched:   return 'Both'
            if val in adb1_only: return 'ADB1 Only'
            return 'ADB2 Only'

        merged['source'] = merged[id_col].apply(_source)

        # Bring 'source' and id_col to the front
        front_cols = [id_col, 'source']
        merged = merged[front_cols + [c for c in merged.columns if c not in front_cols]]

        # Consolidate duplicate name columns (e.g. city_ADB1 / city_ADB2 → city)
        cols_to_process = list(merged.columns)
        for suffix_col in cols_to_process:
            if suffix_col not in merged.columns:
                continue
            for sfx in ('_ADB1', '_ADB2'):
                if suffix_col.endswith(sfx):
                    base = suffix_col[: -len(sfx)]
                    pair = base + ('_ADB2' if sfx == '_ADB1' else '_ADB1')
                    # Only consolidate if neither side already has the base column
                    if base not in merged.columns and pair in merged.columns:
                        merged[base] = merged[suffix_col].combine_first(merged[pair])
                        merged.drop(columns=[suffix_col, pair], inplace=True)
                    break

        # Diagnostics
        print(f"\n  [{label}] Merge summary:")
        print(f"    ADB1 rows  : {len(adb1_df)}")
        print(f"    ADB2 rows  : {len(adb2_df)}")
        print(f"    Matched IDs: {len(matched)}  → {len(matched)} combined rows")
        print(f"    ADB1 only  : {len(adb1_only)} rows (no match in ADB2)")
        print(f"    ADB2 only  : {len(adb2_only)} rows (no match in ADB1)")
        print(f"    Total rows : {len(merged)}")

        return merged

    # 1. Project Wise Merge (key: proj_id)
    try:
        adb1_proj = pd.read_sql_table("ADB1_project_merged", con=engine)
        adb2_proj = pd.read_sql_table("ADB2_project_wise_summary", con=engine)
        grand_proj = id_based_outer_merge(adb1_proj, adb2_proj, "proj_id", "Project")
        grand_proj.to_sql("Grand_project_wise", con=engine, if_exists='replace', index=False)
        print("  ✓ Saved Grand_project_wise")
    except Exception as e:
        print(f"  ⚠ Failed to merge project data: {e}")

    # 2. Location Wise Merge (key: loc_id)
    try:
        adb1_loc = pd.read_sql_table("ADB1_location_merged", con=engine)
        adb2_loc = pd.read_sql_table("ADB2_location_overview_combined", con=engine)
        grand_loc = id_based_outer_merge(adb1_loc, adb2_loc, "loc_id", "Location")
        grand_loc.to_sql("Grand_location_wise", con=engine, if_exists='replace', index=False)
        print("  ✓ Saved Grand_location_wise")
    except Exception as e:
        print(f"  ⚠ Failed to merge location data: {e}")

    # 3. City Wise Merge (key: city_id)
    try:
        adb1_city = pd.read_sql_table("ADB1_city_merged", con=engine)
        adb2_city = pd.read_sql_table("ADB2_city_overview_combined", con=engine)
        grand_city = id_based_outer_merge(adb1_city, adb2_city, "city_id", "City")
        grand_city.to_sql("Grand_city_wise", con=engine, if_exists='replace', index=False)
        print("  ✓ Saved Grand_city_wise")
    except Exception as e:
        print(f"  ⚠ Failed to merge city data: {e}")

def main():
    print("\n" + "*"*60)
    print("  REAL ESTATE PIPELINE ORCHESTRATOR")
    print("*"*60 + "\n")

    # 1. Fresh vs Replace Mode
    print("Database Update Mode:")
    print("  1. Fresh (Delete existing tables and create new)")
    print("  2. Replace (Simple table replacement/update)")
    db_choice = input("Select mode [2]: ").strip() or "2"
    os.environ["ORCHESTRATOR_DB_MODE"] = "fresh" if db_choice == "1" else "replace"

    # 2. City Selection
    cities_list = get_available_cities()
    if cities_list:
        print(f"\nAvailable Cities:")
        for i, city in enumerate(cities_list, 1):
            print(f"  {i}. {city}")
        print(f"  {len(cities_list)+1}. ALL")
        
        city_choice = input(f"\nSelect cities (e.g. 1,3) or {len(cities_list)+1} for ALL: ").strip()
        if not city_choice or city_choice == str(len(cities_list)+1):
            selected_cities = "ALL"
        else:
            try:
                idxs = [int(x.strip())-1 for x in city_choice.split(",")]
                selected_cities = [cities_list[i] for i in idxs if 0 <= i < len(cities_list)]
            except:
                selected_cities = "ALL"
    else:
        print("\n⚠ No cities detected in database (or connection skipped).")
        selected_cities = input("Enter city names separated by commas or press Enter for ALL: ").strip() or "ALL"
        if selected_cities != "ALL":
            selected_cities = [c.strip() for c in selected_cities.split(",") if c.strip()]

    # 3. Pipeline Selection
    print("\nSelect Pipeline to Run:")
    print("  1. ADB1 Only")
    print("  2. ADB2 Only")
    print("  3. ADB1 + ADB2 (Grand Merge - From existing SQL data)")
    print("  4. ALL (ADB1 + ADB2 Pipelines + Grand Merge)")
    run_choice = input("Your choice [4]: ").strip() or "4"

    start_time = time.time()

    if run_choice == "1":
        print(f"\n🚀 Running ADB1 Only...")
        run_adb1(selected_cities)
    elif run_choice == "2":
        print(f"\n🚀 Running ADB2 Only...")
        run_adb2(selected_cities)
    elif run_choice == "3":
        print(f"\n🚀 Checking for existing data for Grand Merge...")
        ok, missing = check_source_tables_exist()
        if ok:
            print("  ✓ All source tables found in DB.")
            merge_and_save_grand()
        else:
            print(f"  ⚠ Missing source data: {missing}")
            print("  Falling back to running BOTH pipelines first...")
            run_choice = "4" # Trigger ALL flow
            
    if run_choice == "4":
        print(f"\n🚀 Starting ADB1 and ADB2 Simultaneously...")
        with ProcessPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(run_adb1, selected_cities)
            f2 = executor.submit(run_adb2, selected_cities)
            f1.result()
            f2.result()
        merge_and_save_grand()

    print(f"\n{'='*60}")
    print(f"  ORCHESTRATOR FINISHED in {time.time()-start_time:.1f}s")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
