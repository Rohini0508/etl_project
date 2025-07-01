import pandas as pd
import configparser
from sqlalchemy import create_engine
import urllib

def extract_data():
    try:
        # Read config
        config = configparser.ConfigParser()
        config.read('./config/config.ini')

        if 'SQL_SERVER' not in config:
            raise KeyError("Missing SQL_SERVER config section")

        sql_cfg = config['SQL_SERVER']

        driver = sql_cfg.get('driver', 'ODBC Driver 17 for SQL Server')
        server = sql_cfg['server']
        database = sql_cfg['database']
        username = sql_cfg['username']
        password = sql_cfg['password']

        # URL encode password and driver for connection string
        password_enc = urllib.parse.quote_plus(password)
        driver_enc = urllib.parse.quote_plus(driver)

        # Build connection string for SQLAlchemy using pyodbc driver
        conn_str = (
            f"mssql+pyodbc://{username}:{password_enc}@{server}/{database}"
            f"?driver={driver_enc}"
        )

        engine = create_engine(conn_str)
        print("✅ Connected to SQL Server")

        # Query your table - update table name if needed
        query = "SELECT * FROM customers_cleanednall"
        df = pd.read_sql(query, engine)

        print(f"📥 Extracted {len(df)} rows from SQL Server")
        return df

    except KeyError as ke:
        print(f"Error in extraction: Missing SQL Server config key: {ke}")
        raise
    except Exception as e:
        print(f"Error in extraction: {e}")
        raise


def extract_mysql_table(table_name):
    try:
        config = configparser.ConfigParser()
        config.read('./config/config.ini')

        if 'MYSQL' not in config:
            raise KeyError("Missing MYSQL config section")

        mysql_cfg = config['MYSQL']

        user = mysql_cfg['username']
        password = mysql_cfg['password']
        host = mysql_cfg['host']
        database = mysql_cfg['database']
        port = mysql_cfg.get('port', '3306')

        password_enc = urllib.parse.quote_plus(password)
        conn_str = f"mysql+pymysql://{user}:{password_enc}@{host}:{port}/{database}"

        engine = create_engine(conn_str)
        print(f"✅ Connected to MySQL for table {table_name}")

        df = pd.read_sql_table(table_name, engine)
        print(f"📥 Extracted {len(df)} rows from MySQL table {table_name}")
        return df

    except KeyError as ke:
        print(f"Error in extraction: Missing MySQL config key: {ke}")
        raise
    except Exception as e:
        print(f"Error extracting table {table_name}: {e}")
        # Return empty DataFrame with no rows but columns could be inferred or empty
        return pd.DataFrame()
