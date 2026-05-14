from db_config import engine
import pandas as pd
import os

def clean_sqlite_table():
    """
    Explicitly reads defined tables from staging, cleans, renames columns to
    match join keys, handles currency, and saves to transformation layer.
    """
    base_dir = os.getcwd()
    staging_dir = os.path.join(base_dir, 'data', 'staging')
    sources = [
        {'db': 'japan staging area.db', 'loc': 'Japan', 'currency': 'JPY', 'prefix': 'japan'},
        {'db': 'myanmar staging area.db', 'loc': 'Myanmar', 'currency': 'USD', 'prefix': 'myanmar'}
    ]

    table_types = ['branch', 'customers', 'items', 'payment', 'sales_data']
    
    JPY_RATE = 0.0067 

    for source in sources:
        
        for table_type in table_types:
            # Table name in Staging DB (e.g., 'japan_sales_data')
            table_name_in_staging = f"{source['prefix']}_{table_type}"
            
            print(f"Transforming {table_name_in_staging} ({source['loc']})...")

            try:
                df = pd.read_sql(f'SELECT * FROM "{table_name_in_staging}"', engine)
            except pd.io.sql.DatabaseError:
                print(f"  -> ERROR: Table '{table_name_in_staging}' not found. Did extract.py run correctly?")
                continue 

            # --- GENERAL CLEANING ---
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            # --- DEFENSIVE COLUMN RENAMING & LOGIC ---
            
            if 'branch' == table_type:
                # Renames 'id' to 'branch_id'
                df.rename(columns={'id': 'branch_id', 'name': 'branch_name'}, inplace=True)
                print("  -> Renamed 'id' to 'branch_id'")
                
            elif 'customers' == table_type:
                # Renames 'id' to 'customer_id'
                df.rename(columns={'id': 'customer_id', 'name': 'customer_name'}, inplace=True)
                print("  -> Renamed 'id' to 'customer_id'")
                
            elif 'payment' == table_type:
                # Renames 'id' to 'payment' for joining with sales_data
                df.rename(columns={'id': 'payment', 'name': 'payment_method'}, inplace=True)
                print("  -> Renamed 'id' to 'payment'")
                
            elif 'items' == table_type:
                # Renames 'id' to 'product_id'
                if 'id' in df.columns:
                    df.rename(columns={'id': 'product_id'}, inplace=True)
                    print("  -> Renamed 'id' to 'product_id'")
                
                # CURRENCY CONVERSION (Only for Japan)
                if source['currency'] == 'JPY':
                    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
                    df['price'] = df['price'] * JPY_RATE
                    print("  -> Converted Price JPY to USD")

            elif 'sales_data' == table_type:
                # Check for item_id or similar common alternatives and standardize to product_id
                # (Assuming product_id is already correct per your prompt, but checking for safety)
                if 'item_id' in df.columns and 'product_id' not in df.columns:
                     df.rename(columns={'item_id': 'product_id'}, inplace=True)
                     print("  -> Renamed 'item_id' to 'product_id'")
                # No change needed if it's already 'product_id'

            # --- SAVE to Transformation DB ---
            target_table_name = f"{table_name_in_staging}_cleaned"
            df.to_sql(target_table_name, engine, if_exists='replace', index=False)
            
    print("Transformation Layer Complete. All tables saved with '_cleaned' suffix.")

if __name__ == "__main__":
    clean_sqlite_table()