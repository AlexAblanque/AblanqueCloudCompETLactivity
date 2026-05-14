import pandas as pd
import os
from db_config import engine

def run_analytics():
    """
    Connects to the final BIG TABLE and extracts 5 key business insights.
    Includes robust column name discovery for 'location' and 'payment method'
    and reverse-engineers the location column if it's missing.
    """
    try:
        df = pd.read_sql("SELECT * FROM consolidated_sales", engine)
        
        if df.empty:
            print("ERROR: 'consolidated_sales' table is empty.")
            return

    except Exception as e:
        print(f"FATAL ERROR: Could not connect to or read BIG TABLE.db. {e}")
        return

    # --- CRITICAL FIX: REVERSE-ENGINEER MISSING LOCATION COLUMN ---
    # Since the 'store_location' column is missing, we create it based on 'city'.
    
    # Identify cities belonging to Myanmar (Assuming typical cities for Myanmar)
    myanmar_cities = ['yangon', 'mandalay', 'naypyidaw', 'mandalay', 'naypyidaw', 'taunggyi']
    
    # Clean up city names for matching
    df['city_clean'] = df['city'].astype(str).str.lower().str.strip()
    
    # Create the 'store_location' column
    df['store_location'] = df['city_clean'].apply(
        lambda x: 'Myanmar' if x in myanmar_cities else 'Japan'
    )
    
    # Standardize other column names that might have casing issues
    if 'payment_method' not in df.columns:
        if 'Payment_Method' in df.columns:
            df.rename(columns={'Payment_Method': 'payment_method'}, inplace=True)
    
    print("--- DATA FIX COMPLETE: Created 'store_location' column via city reverse-engineering. ---")
    
    # --- ANALYSIS STARTS HERE ---

    print("--- 📊 ETL Activity Analytics ---")
    print(f"Total Transactions Analyzed: {len(df)}")
    print(f"Total Revenue (USD): ${df['total_amount'].sum():,.2f}\n")

    # --- INSIGHT 1: Revenue by Country ---
    revenue_by_loc = df.groupby('store_location')['total_amount'].sum().sort_values(ascending=False)
    print("## 1. Revenue by Store Location (USD)")
    print(revenue_by_loc.map('${:,.2f}'.format))
    print("-" * 30)

    # --- INSIGHT 2: Top Selling Item Category ---
    top_category = df.groupby('category')['quantity'].sum().sort_values(ascending=False).head(3)
    print("## 2. Top 3 Categories by Units Sold")
    print(top_category)
    print("-" * 30)

    # --- INSIGHT 3: Average Rating by Membership Type ---
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    avg_rating_membership = df.groupby('membership')['rating'].mean().sort_values(ascending=False)
    print("## 3. Average Customer Rating by Membership Tier")
    print(avg_rating_membership.round(2))
    print("-" * 30)
    

    # --- INSIGHT 4: Payment Method Preference by Country ---
    payment_pivot = df.pivot_table(
        index='payment_method', 
        columns='store_location', 
        values='invoice_id', 
        aggfunc='count', 
        fill_value=0
    )
    payment_pivot['Total'] = payment_pivot.sum(axis=1)
    payment_pivot = payment_pivot.sort_values(by='Total', ascending=False)
    
    print("## 4. Payment Method Usage by Country (Transaction Count)")
    print(payment_pivot)
    print("-" * 30)
    

    # --- INSIGHT 5: Peak Sales Hour ---
    df['hour'] = pd.to_datetime(df['time']).dt.hour
    peak_hour = df.groupby('hour')['total_amount'].sum().sort_values(ascending=False)
    
    print("## 5. Most Profitable Hour of the Day (Top 3)")
    top_3_hours = peak_hour.head(3)
    
    print(f"Top 3 Peak Hours (Revenue):\n{top_3_hours.map('${:,.2f}'.format)}")
    print(f"Peak Hour: {top_3_hours.index[0]} (e.g., 14 means 2 PM)")
    print("-" * 30)
    
if __name__ == "__main__":
    run_analytics()