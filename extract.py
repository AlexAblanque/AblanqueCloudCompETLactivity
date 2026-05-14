import pandas as pd
import os
from db_config import engine

def load_csv():
    """
    Reads CSVs and creates unique table names in staging DBs based on file name logic.
    Assumes: sales_data.csv, OR {store}_branch.csv, etc.
    """
    base_dir = os.getcwd()
    store_configs = {
        'japan_store': {'db_name': 'japan staging area.db', 'prefix': 'japan'},
        'myanmar_store': {'db_name': 'myanmar staging area.db', 'prefix': 'myanmar'}
    }

    for folder, config in store_configs.items():
        source_path = os.path.join(base_dir, 'data', 'source', folder)
        print(f"--- Processing {folder} -> {config['db_name']} ---")

        if os.path.exists(source_path):
            for file in os.listdir(source_path):
                if file.endswith('.csv'):
                    base_name = os.path.splitext(file)[0]
                    
                    # 1. Determine the unique table name to save in the Staging DB
                    if base_name == 'sales_data':
                        # File is 'sales_data.csv' -> Table name must be 'japan_sales_data'
                        unique_table_name = f"{config['prefix']}_{base_name}"
                    else:
                        # File is likely 'japan_branch.csv', 'myanmar_customers.csv', etc.
                        # The base_name already contains the prefix, so use it as is.
                        unique_table_name = base_name 
                    
                    file_full_path = os.path.join(source_path, file)
                    
                    try:
                        df = pd.read_csv(file_full_path)
                        df.to_sql(unique_table_name, engine, if_exists='replace', index=False)
                        print(f"Loaded: {unique_table_name}")
                    except Exception as e:
                        print(f"Error loading {file}: {e}")
        else:
            print(f"Folder not found: {source_path}")
        

if __name__ == "__main__":
    load_csv()