import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import io

# --- DB CONNECTION ---
DB_URL = st.secrets["SUPABASE_URL"]

engine = create_engine(
    DB_URL,
    connect_args={"sslmode": "require"},
    pool_pre_ping=True
)

# --- INIT DB ---
def init_db():
    with engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                order_id TEXT,
                source TEXT,
                channel TEXT,
                order_timestamp TIMESTAMP,
                time_of_day TEXT,
                vat_rate TEXT,
                net_sales DOUBLE PRECISION,
                tax DOUBLE PRECISION,
                gross_sales DOUBLE PRECISION,
                UNIQUE(order_id, vat_rate)
            )
        '''))

# --- HELPERS ---
def get_time_of_day(hour):
    if pd.isna(hour): return 'Unknown'
    return 'Lunch' if hour < 16 else 'Dinner'

# --- LIGHTSPEED PROCESSOR (FIXED VAT SPLIT) ---
def process_lightspeed(df, version="K-Series"):
    if 'Status' in df.columns:
        df = df[df['Status'].isin(['Paid', 'Done'])].copy()

    receipt_col = 'Receipt ID' if 'Receipt ID' in df.columns else df.columns[0]
    date_col = 'Creation Date' if 'Creation Date' in df.columns else 'Date'

    df['order_id'] = f'LS_{version}_' + df[receipt_col].astype(str)
    df['channel'] = df['Type'].apply(
        lambda x: 'Takeaway' if str(x).lower() == 'takeaway' else 'In-Restaurant'
    ) if 'Type' in df.columns else 'In-Restaurant'

    df['order_timestamp'] = pd.to_datetime(df[date_col], dayfirst=True)
    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['source'] = f'Lightspeed {version}'

    rows = []

    for _, r in df.iterrows():
        vat_string = str(r.get('Taxes', '')).strip()

        if '=' in vat_string:
            parts = [p.strip() for p in vat_string.split('|')]

            for part in parts:
                try:
                    rate_part, tax_part = part.split('=')

                    rate = float(rate_part.replace('%','').strip())
                    tax = float(tax_part.strip())

                    net = round(tax / (rate / 100), 2) if rate != 0 else 0
                    gross = round(net + tax, 2)

                    rows.append({
                        'order_id': r['order_id'],
                        'source': r['source'],
                        'channel': r['channel'],
                        'order_timestamp': r['order_timestamp'],
                        'time_of_day': r['time_of_day'],
                        'vat_rate': f"{int(rate)}%",
                        'net_sales': net,
                        'tax': tax,
                        'gross_sales': gross
                    })

                except Exception:
                    continue
        else:
            gross = pd.to_numeric(r.get('Total', 0), errors='coerce')
            net = pd.to_numeric(r.get('Net Total', 0), errors='coerce')
            tax = gross - net

            rows.append({
                'order_id': r['order_id'],
                'source': r['source'],
                'channel': r['channel'],
                'order_timestamp': r['order_timestamp'],
                'time_of_day': r['time_of_day'],
                'vat_rate': 'Mixed',
                'net_sales': net,
                'tax': tax,
                'gross_sales': gross
            })

    clean_df = pd.DataFrame(rows)

    # sanity check
    if not clean_df.empty:
        orig = pd.to_numeric(df['Total'], errors='coerce').sum()
        new = clean_df['gross_sales'].sum()

        if abs(orig - new) > 1:
            print(f"⚠️ WARNING totals mismatch {orig} vs {new}")

    return clean_df

# --- OTHER SOURCES ---
def process_ubereats(df):
    df = df[df['Order status'] == 'Completed'].copy()
    df['order_id'] = 'UE_' + df['Order ID'].astype(str)
    df['channel'] = 'Delivery'
    df['source'] = 'Uber Eats'
    df['order_timestamp'] = pd.to_datetime(df['Order date'] + ' ' + df['Order confirmed time'], dayfirst=True)
    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)

    df['gross_sales'] = pd.to_numeric(df['Sales (incl. VAT)'], errors='coerce').fillna(0)
    df['net_sales'] = pd.to_numeric(df['Sales (excl. VAT)'], errors='coerce').fillna(0)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'

    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']]

def process_deliveroo(df):
    df = df[df['Order status'] == 'Completed'].copy()
    df['order_id'] = 'DL_' + df['Order number'].astype(str)
    df['channel'] = 'Delivery'
    df['source'] = 'Deliveroo'

    if 'Time submitted' in df.columns:
        df['order_timestamp'] = pd.to_datetime(df['Date submitted'] + ' ' + df['Time submitted'], dayfirst=True)
    else:
        df['order_timestamp'] = pd.to_datetime(df['Date submitted'], dayfirst=True)

    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['gross_sales'] = pd.to_numeric(df['Subtotal'], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'

    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']]

def process_takeaway(df):
    df['order_id'] = 'TA_' + df['Order'].astype(str)
    df['channel'] = 'Delivery'
    df['source'] = 'Takeaway'
    df['order_timestamp'] = pd.to_datetime(df['Date'], dayfirst=True)
    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)

    df['gross_sales'] = pd.to_numeric(df['Total amount'], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'

    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']]

# --- SAVE ---
def save_to_db(clean_df):
    if clean_df.empty:
        return 0, 0

    total_rows = len(clean_df)
    unique_orders = clean_df['order_id'].nunique()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sales (order_id, source, channel, order_timestamp, time_of_day, vat_rate, net_sales, tax, gross_sales)
            VALUES (:order_id, :source, :channel, :order_timestamp, :time_of_day, :vat_rate, :net_sales, :tax, :gross_sales)
            ON CONFLICT (order_id, vat_rate) DO NOTHING
        """), clean_df.to_dict(orient="records"))

    return total_rows, unique_orders

# --- LOAD ---
@st.cache_data(ttl=60)
def load_data():
    df = pd.read_sql("SELECT * FROM sales", engine)
    if df.empty:
        return df

    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'])
    df['order_date'] = df['order_timestamp'].dt.date
    df['year'] = df['order_timestamp'].dt.year
    df['month'] = df['order_timestamp'].dt.to_period('M').astype(str)
    df['week_str'] = df['order_timestamp'].dt.strftime('%G-W%V')
    df['quarter'] = df['order_timestamp'].dt.to_period('Q').astype(str)

    return df

# --- APP ---
st.set_page_config(page_title="Restaurant OS", layout="wide")
init_db()

st.sidebar.title("Data Sync")

source_option = st.sidebar.selectbox("Source", [
    "Lightspeed K-Series","Lightspeed L-Series","Deliveroo","Uber Eats","Takeaway"
])

uploaded_file = st.sidebar.file_uploader("Upload CSV", type=["csv"])

data = load_data()

# Show data range
if not data.empty:
    src_data = data[data['source'] == source_option]
    if not src_data.empty:
        st.sidebar.info(f"Data in DB: {src_data['order_timestamp'].min().date()} → {src_data['order_timestamp'].max().date()}")

if st.sidebar.button("Process File"):
    if uploaded_file:
        raw = uploaded_file.read()
        df_raw = pd.read_csv(io.BytesIO(raw), sep=';')

        parsers = {
            "Lightspeed K-Series": lambda d: process_lightspeed(d, "K-Series"),
            "Lightspeed L-Series": lambda d: process_lightspeed(d, "L-Series"),
            "Uber Eats": process_ubereats,
            "Deliveroo": process_deliveroo,
            "Takeaway": process_takeaway,
        }

        clean_df = parsers[source_option](df_raw)

        rows, orders = save_to_db(clean_df)

        st.sidebar.success(f"""
        ✅ Import completed  
        • {orders} orders  
        • {rows} VAT lines
        """)

        st.cache_data.clear()

# --- DASHBOARD ---
tab1, tab2 = st.tabs(["Dashboard", "VAT Report"])

with tab1:
    if not data.empty:
        st.metric("Revenue", f"€{data['gross_sales'].sum():,.2f}")

with tab2:
    if not data.empty:
        summary = data.groupby(['source','vat_rate'])[['net_sales','tax','gross_sales']].sum().reset_index()
        st.dataframe(summary)
