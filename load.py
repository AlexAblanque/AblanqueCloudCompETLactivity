import pandas as pd
from db_config import engine
import os

def load_presentation():
    """
    Joins Sales + Branch + Customers + Items + Payment into one BIG TABLE.
    Includes a critical column cleaning step to remove extra quotes.
    """
    base_dir = os.getcwd()

    locations = ['Japan', 'Myanmar']
    all_data = []

    for loc in locations:
        print(f"\n--- Attempting to merge data for {loc} ---")
        
        prefix = loc.lower() 
        table_config = {
            'sales': f'{prefix}_sales_data_cleaned',
            'items': f'{prefix}_items_cleaned',
            'customers': f'{prefix}_customers_cleaned',
            'branch': f'{prefix}_branch_cleaned',
            'payment': f'{prefix}_payment_cleaned',
        }
        
        try:
            # Read all tables
            df_sales = pd.read_sql(f'SELECT * FROM "{table_config['sales']}"', engine)
            df_items = pd.read_sql(f'SELECT * FROM "{table_config['items']}"', engine)
            df_cust = pd.read_sql(f'SELECT * FROM "{table_config['customers']}"', engine)
            df_branch = pd.read_sql(f'SELECT * FROM "{table_config['branch']}"', engine)
            df_pay = pd.read_sql(f'SELECT * FROM "{table_config['payment']}"', engine)
            print(f"Source tables for {loc} read successfully.")

            # --- CRITICAL FIX: STRIP EXTRA QUOTES FROM COLUMN NAMES ---
            # This is necessary because some column names were stored as strings
            # including quotes (e.g., "'product_id'") in the DB.
            df_sales.columns = df_sales.columns.str.strip("'")
            df_items.columns = df_items.columns.str.strip("'")
            df_cust.columns = df_cust.columns.str.strip("'")
            df_branch.columns = df_branch.columns.str.strip("'")
            df_pay.columns = df_pay.columns.str.strip("'")
            print("--- COLUMN CLEANUP SUCCESS: Removed spurious single quotes from headers. ---")
            
            # 1. Sales + Items (on 'product_id') - This should work now
            merged = pd.merge(df_sales, df_items, on='product_id', how='left', suffixes=('', '_drop'))

            # 2. + Customers (on 'customer_id')
            merged = pd.merge(merged, df_cust, on='customer_id', how='left', suffixes=('', '_drop'))

            # 3. + Branch (on 'branch_id')
            merged = pd.merge(merged, df_branch, on='branch_id', how='left', suffixes=('', '_drop'))

            # 4. + Payment (on 'payment')
            merged = pd.merge(merged, df_pay, on='payment', how='left', suffixes=('', '_drop'))

            # Clean up duplicate columns
            merged = merged[[c for c in merged.columns if not c.endswith('_drop')]]
            
            all_data.append(merged)
            print(f"Data for {loc} merged successfully. Final rows: {len(merged)}")

        except Exception as e:
            print(f"GENERAL ERROR merging {loc}: {e}")
            

    if all_data:
        big_table = pd.concat(all_data, ignore_index=True)
        
        # Calculate Total Amount (Quantity * Price)
        # We must also ensure 'quantity' and 'price' are numeric
        big_table['quantity'] = pd.to_numeric(big_table['quantity'], errors='coerce').fillna(0)
        big_table['price'] = pd.to_numeric(big_table['price'], errors='coerce').fillna(0)

        big_table['total_amount'] = big_table['quantity'] * big_table['price']
        
        big_table.to_sql('consolidated_sales', engine, if_exists='replace', index=False)
        print("\nSUCCESS: BIG TABLE created and saved as 'consolidated_sales'.")
        print(f"Total Rows in Final Table: {len(big_table)}")
    else:
        print("\nFATAL: No data was collected.")


if __name__ == "__main__":
    load_presentation()