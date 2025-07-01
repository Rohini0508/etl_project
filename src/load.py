import pandas as pd
import configparser
import urllib.parse
from sqlalchemy import create_engine

from extract import extract_data
from transform import (
    scd_type0, scd_type1, scd_type2, scd_type3, scd_type4,
    sort_customers_by_registration_date,
    aggregate_customers_by_loyalty
)

def load_to_mysql(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Loaded {table_name}: {len(df)} rows")
    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")

def main():
    print("🚀 Starting full ETL pipeline...")

    # Step 1: Extract data from SQL Server
    try:
        data = extract_data()
        print(f"📥 Extracted {len(data)} rows from SQL Server\n")
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return

    # Step 2: Define transformation setup
    key_col = 'customer_id'
    tracked_cols = ['name', 'email', 'phone', 'address', 'loyalty_status']

    dim_df_basic = pd.DataFrame(columns=[key_col] + tracked_cols)
    dim_df_full = pd.DataFrame(columns=[key_col] + tracked_cols + ['start_date', 'end_date', 'is_current'])
    history_df = pd.DataFrame(columns=dim_df_full.columns)

    # Step 3: Apply SCD transformations
    dim_scd0 = scd_type0(dim_df_basic, data, key_col, tracked_cols)
    dim_scd1 = scd_type1(dim_df_basic, data, key_col, tracked_cols)
    dim_scd2 = scd_type2(dim_df_full, data, key_col, tracked_cols)
    dim_scd3 = scd_type3(dim_df_basic, data, key_col, tracked_cols)
    dim_scd4, history_df = scd_type4(dim_df_full, data, key_col, tracked_cols, history_df)

    # Step 4: Perform analytics
    sorted_customers = sort_customers_by_registration_date(data)
    loyalty_summary = aggregate_customers_by_loyalty(data)

    # Step 5: Read MySQL config
    config = configparser.ConfigParser()
    config.read('./config/config.ini')

    try:
        mysql_host = config['MYSQL']['host']
        mysql_db = config['MYSQL']['database']
        mysql_user = config['MYSQL']['username']
        mysql_pass = config['MYSQL']['password']
        mysql_port = config['MYSQL'].get('port', '3306')
    except KeyError as e:
        print(f"❌ Missing MySQL config key: {e}")
        return

    encoded_pass = urllib.parse.quote_plus(mysql_pass)
    conn_str = f'mysql+pymysql://{mysql_user}:{encoded_pass}@{mysql_host}:{mysql_port}/{mysql_db}'

    # Step 6: Create MySQL engine
    try:
        engine = create_engine(conn_str)
        print("✅ Connected to MySQL\n")
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        return

    # Step 7: Load all datasets
    load_to_mysql(dim_scd0, 'dim_scd0', engine)
    load_to_mysql(dim_scd1, 'dim_scd1', engine)
    load_to_mysql(dim_scd2, 'dim_scd2', engine)
    load_to_mysql(dim_scd3, 'dim_scd3', engine)
    load_to_mysql(dim_scd4, 'dim_scd4', engine)
    load_to_mysql(history_df, 'history_scd4', engine)
    load_to_mysql(sorted_customers, 'sorted_customers', engine)
    load_to_mysql(loyalty_summary, 'loyalty_summary', engine)

    print("\n✅ ETL process completed and all tables loaded into MySQL successfully.")

if __name__ == '__main__':
    main()
