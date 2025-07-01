import configparser
import urllib.parse
from sqlalchemy import create_engine

from extract import extract_data, extract_mysql_table
from transform import (
    scd_type0, scd_type1, scd_type2, scd_type3, scd_type4,
    sort_customers_by_registration_date, aggregate_customers_by_loyalty
)
from load import load_to_mysql

def main():
    print("🚀 ETL Starting...")

    # Step 1: Extract data from SQL Server
    try:
        source = extract_data()
        print(f"📥 SQL Server rows: {len(source)}\n")
    except Exception as e:
        print(f"❌ Failed to extract from SQL Server: {e}")
        return

    # Step 2: Load MySQL config
    cfg = configparser.ConfigParser()
    cfg.read('./config/config.ini')
    try:
        m = cfg['MYSQL']
        user = m['username']
        pwd = urllib.parse.quote_plus(m['password'])
        host = m['host']
        db = m['database']
        port = m.get('port', '3306')
    except KeyError as e:
        print(f"❌ Missing config value: {e}")
        return

    # Step 3: Create MySQL engine
    try:
        conn_str = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
        engine = create_engine(conn_str)
        print("✅ MySQL Connected\n")
    except Exception as e:
        print(f"❌ Failed to connect to MySQL: {e}")
        return

    # Step 4: Extract existing MySQL dimension tables
    dims0 = extract_mysql_table('dim_scd0')
    dims1 = extract_mysql_table('dim_scd1')
    dims2 = extract_mysql_table('dim_scd2')
    dims3 = extract_mysql_table('dim_scd3')
    dims4 = extract_mysql_table('dim_scd4')
    hist4 = extract_mysql_table('history_scd4')

    # Step 5: Apply SCD Transformations
    k = 'customer_id'
    cols = ['name', 'email', 'phone', 'address', 'loyalty_status']

    d0 = scd_type0(dims0, source, k, cols)
    d1 = scd_type1(dims1, source, k, cols)
    d2 = scd_type2(dims2, source, k, cols)
    d3 = scd_type3(dims3, source, k, cols)
    d4, h4 = scd_type4(dims4, source, k, cols, hist4)

    # Step 6: Analytics
    try:
        sorted_c = sort_customers_by_registration_date(source)
        loyalty = aggregate_customers_by_loyalty(source)
    except Exception as e:
        print(f"❌ Failed in analytics step: {e}")
        return

    # Step 7: Load to MySQL
    load_to_mysql(d0, 'dim_scd0', engine)
    load_to_mysql(d1, 'dim_scd1', engine)
    load_to_mysql(d2, 'dim_scd2', engine)
    load_to_mysql(d3, 'dim_scd3', engine)
    load_to_mysql(d4, 'dim_scd4', engine)
    load_to_mysql(h4, 'history_scd4', engine)
    load_to_mysql(sorted_c, 'sorted_customers', engine)
    load_to_mysql(loyalty, 'loyalty_summary', engine)

    print("\n✅ ETL Completed Successfully")

if __name__ == "__main__":
    main()
