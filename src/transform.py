import pandas as pd
from datetime import datetime
from extract import extract_data  # Make sure extract.py is in the same folder or in PYTHONPATH

# -------------------
# SCD TYPE FUNCTIONS
# -------------------
#task 9
def scd_type0(dim_df, new_df, key_col, tracked_cols):
    existing = set(dim_df[key_col]) if not dim_df.empty else set()
    adds = new_df[~new_df[key_col].isin(existing)]
    return pd.concat([dim_df, adds], ignore_index=True)

def scd_type1(dim_df, new_df, key_col, tracked_cols):
    dim = dim_df.copy()
    new = new_df.copy()
    mask = (new[key_col] == 1) & (new['loyalty_status'].str.lower() == 'gold')
    new.loc[mask, 'loyalty_status'] = 'Diamond'
    dim = dim[~dim[key_col].isin(new[key_col])]
    return pd.concat([dim, new], ignore_index=True)

def scd_type2(dim_df, new_df, key_col, tracked_cols):
    today = datetime.today().strftime('%Y-%m-%d')
    dim = dim_df.copy()
    new = new_df.copy()
    if dim.empty:
        new['start_date'] = today
        new['end_date'] = None
        new['is_current'] = True
        return new

    curr = dim[dim['is_current'] == True]
    merged = pd.merge(new, curr, on=key_col, how='left', suffixes=('_new', '_old'), indicator=True)
    mask = False
    for c in tracked_cols:
        mask |= merged[f"{c}_new"] != merged[f"{c}_old"]
    mask |= merged['_merge'] == 'left_only'

    updates = merged[mask]
    for _, row in updates.iterrows():
        key = row[key_col]
        if row['_merge'] == 'both':
            dim.loc[(dim[key_col] == key) & (dim['is_current'] == True), 'is_current'] = False
            dim.loc[(dim[key_col] == key) & (dim['is_current'] == False), 'end_date'] = today
        newrec = {c: row[f"{c}_new"] for c in tracked_cols}
        newrec[key_col] = key
        newrec.update({'start_date': today, 'end_date': None, 'is_current': True})
        dim = pd.concat([dim, pd.DataFrame([newrec])], ignore_index=True)
    return dim

def scd_type3(dim_df, new_df, key_col, tracked_cols, history_suffix='_prev'):
    dim = dim_df.copy()
    new = new_df.copy()
    if dim.empty:
        for c in tracked_cols:
            new[c + history_suffix] = None
        return new

    merged = pd.merge(new, dim, on=key_col, how='left', suffixes=('', '_old'))
    for c in tracked_cols:
        prev = c + history_suffix
        merged[prev] = merged.apply(
            lambda r: r[c + '_old'] if pd.notna(r[c + '_old']) and r[c] != r[c + '_old'] else None,
            axis=1
        )
    drop = [col for col in merged if col.endswith('_old')]
    return merged.drop(columns=drop)

def scd_type4(dim_df, new_df, key_col, tracked_cols, hist_df):
    today = datetime.today().strftime('%Y-%m-%d')
    dim = dim_df.copy()
    new = new_df.copy()
    if hist_df is None:
        hist_df = pd.DataFrame(columns=dim.columns)

    merged = pd.merge(new, dim, on=key_col, how='left', suffixes=('_new', '_old'), indicator=True)
    mask = False
    for c in tracked_cols:
        mask |= merged[f"{c}_new"] != merged[f"{c}_old"]
    mask |= merged['_merge'] == 'left_only'
    ch = merged[mask]

    for _, r in ch.iterrows():
        key = r[key_col]
        old = dim[dim[key_col] == key]
        if not old.empty:
            old = old.copy()
            old['end_date'] = today
            hist_df = pd.concat([hist_df, old], ignore_index=True)

    dim = dim[~dim[key_col].isin(ch[key_col])]
    ins = ch[[key_col] + [f"{c}_new" for c in tracked_cols]].copy()
    ins.columns = [key_col] + tracked_cols
    ins['start_date'] = today
    ins['end_date'] = None
    ins['is_current'] = True
    dim = pd.concat([dim, ins], ignore_index=True)
    return dim, hist_df

# -------------------
# ANALYTICAL FUNCTIONS
# -------------------
#task 7
def sort_customers_by_registration_date(df):
    if 'registration_date' not in df.columns:
        raise ValueError("Missing 'registration_date' column.")
    return df.sort_values(by='registration_date')
#task 8
def aggregate_customers_by_loyalty(df):
    if 'loyalty_status' not in df.columns:
        raise ValueError("Missing 'loyalty_status' column.")
    return df.groupby('loyalty_status').agg(total_customers=('customer_id', 'count')).reset_index()

# -------------------
# MAIN TEST SCRIPT
# -------------------

if __name__ == '__main__':
    try:
        print("📥 Extracting data from SQL Server...")
        data = extract_data()
        print(f"✅ Extracted {len(data)} rows\n")

        key_col = 'customer_id'
        tracked_cols = ['name', 'email', 'phone', 'address', 'loyalty_status']

        dim_df_basic = pd.DataFrame(columns=[key_col] + tracked_cols)
        dim_df_full = pd.DataFrame(columns=[key_col] + tracked_cols + ['start_date', 'end_date', 'is_current'])
        history_df = pd.DataFrame(columns=dim_df_full.columns)

        print("🧪 Applying SCD Type 0")
        dim_scd0 = scd_type0(dim_df_basic, data, key_col, tracked_cols)
        print(dim_scd0.head(), "\n")

        print("🧪 Applying SCD Type 1")
        dim_scd1 = scd_type1(dim_df_basic, data, key_col, tracked_cols)
        print(dim_scd1.head(), "\n")

        print("🧪 Applying SCD Type 2")
        dim_scd2 = scd_type2(dim_df_full, data, key_col, tracked_cols)
        print(dim_scd2.head(), "\n")

        print("🧪 Applying SCD Type 3")
        dim_scd3 = scd_type3(dim_df_basic, data, key_col, tracked_cols)
        print(dim_scd3.head(), "\n")

        print("🧪 Applying SCD Type 4")
        dim_scd4, history_df = scd_type4(dim_df_full, data, key_col, tracked_cols, history_df)
        print("📦 Current (SCD4):")
        print(dim_scd4.head(), "\n")
        print("📦 History (SCD4):")
        print(history_df.head(), "\n")

        print("📊 Sorted Customers by Registration Date")
        sorted_customers = sort_customers_by_registration_date(data)
        print(sorted_customers.head(), "\n")

        print("📊 Loyalty Summary")
        loyalty_summary = aggregate_customers_by_loyalty(data)
        print(loyalty_summary.head(), "\n")

        print("✅ All transformations completed successfully.")

    except Exception as e:
        print(f"❌ Error during transformation: {e}")
