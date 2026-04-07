import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import io

# Securely connect to Supabase
DB_URL = st.secrets["SUPABASE_URL"]
engine = create_engine(
    DB_URL,
    connect_args={"sslmode": "require"},
    pool_pre_ping=True
)

def init_db():
    with engine.connect() as conn:
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
        conn.commit()

def get_time_of_day(hour):
    if pd.isna(hour): return 'Unknown'
    return 'Lunch' if hour < 16 else 'Dinner'

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

    return pd.DataFrame(rows)
    
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
    df = df.copy()
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

# ✅ IMPROVED SAVE
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

@st.cache_data(ttl=60)
def load_data():
    df = pd.read_sql("SELECT * FROM sales", engine)
    if not df.empty:
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

with st.sidebar.expander("How to update", expanded=True):
    st.markdown("""
- **K-Series:** Backoffice → Reports → Receipts → Export CSV  
- **Uber Eats:** UE Manager → Payments → Invoices → Export CSV  
- **Deliveroo:** Hub → Invoices → Orders → Export CSV  
- **Takeaway:** Portal → Invoicing → Orders → Export CSV
    """)

source_option = st.sidebar.selectbox("Source", ["Lightspeed K-Series","Lightspeed L-Series","Deliveroo","Uber Eats","Takeaway"])
uploaded_file = st.sidebar.file_uploader("Upload CSV", type=["csv"])

data = load_data()

# ✅ data range info
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

tab_dash, tab_vat = st.tabs(["Management Dashboard", "VAT Report"])
data = load_data()

with tab_dash:
    st.header("Management Dashboard")
    if data.empty:
        st.info("No data yet — upload a CSV from the sidebar.")
    else:
        total = data['gross_sales'].sum()
        days = data['order_date'].nunique() or 1
        lunch = data[data['time_of_day']=='Lunch']['gross_sales'].sum()
        dinner = data[data['time_of_day']=='Dinner']['gross_sales'].sum()

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total revenue", f"€{total:,.2f}")
        c2.metric("Avg per open day", f"€{total/days:,.2f}")
        c3.metric("Lunch", f"€{lunch:,.2f}", f"{lunch/total*100:.1f}%" if total else "")
        c4.metric("Dinner", f"€{dinner:,.2f}", f"{dinner/total*100:.1f}%" if total else "")

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            view = st.radio("Trend view", ["Weekly","Monthly"], horizontal=True)
            grp_col = 'week_str' if view == 'Weekly' else 'month'
            trend = data.groupby([grp_col,'year'])['gross_sales'].sum().reset_index()
            fig = px.bar(trend, x=grp_col, y='gross_sales', color='year', barmode='group')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            by_channel = data.groupby('channel')['gross_sales'].sum().reset_index()
            fig2 = px.pie(by_channel, values='gross_sales', names='channel', hole=0.4)
            st.plotly_chart(fig2, use_container_width=True)

        by_src = data.groupby(['source','time_of_day'])['gross_sales'].sum().reset_index()
        fig3 = px.bar(by_src, x='source', y='gross_sales', color='time_of_day')
        st.plotly_chart(fig3, use_container_width=True)

with tab_vat:
    st.header("VAT Report")
    if data.empty:
        st.info("No data yet.")
    else:
        quarters = sorted(data['quarter'].unique(), reverse=True)
        selected_q = st.selectbox("Quarter", quarters)
        q_data = data[data['quarter'] == selected_q]

        summary = q_data.groupby(['source','vat_rate'])[['net_sales','tax','gross_sales']].sum().reset_index()
        st.dataframe(summary)
