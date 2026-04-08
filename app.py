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

def get_time_of_day(hour):
    if pd.isna(hour): return 'Unknown'
    return 'Lunch' if hour < 16 else 'Dinner'

def process_lightspeed(df, version="K-Series"):
    if 'Status' in df.columns:
        df = df[df['Status'].astype(str).str.strip().isin(['Paid', 'Done'])].copy()

    receipt_col = 'Receipt ID' if 'Receipt ID' in df.columns else df.columns[0]
    
    if 'Finalized Date' in df.columns:
        date_col = 'Finalized Date'
    elif 'Creation Date' in df.columns:
        date_col = 'Creation Date'
    else:
        date_col = 'Date'

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
                        'order_id': r['order_id'], 'source': r['source'], 'channel': r['channel'],
                        'order_timestamp': r['order_timestamp'], 'time_of_day': r['time_of_day'],
                        'vat_rate': f"{int(rate)}%", 'net_sales': net, 'tax': tax, 'gross_sales': gross
                    })
                except Exception:
                    continue
        else:
            gross = pd.to_numeric(r.get('Total') if pd.notna(r.get('Total')) else r.get('Total incl. Tax', 0), errors='coerce') or 0.0
            net = pd.to_numeric(r.get('Net Total') if pd.notna(r.get('Net Total')) else r.get('Total excl. Tax', 0), errors='coerce') or 0.0
            tax = gross - net

            rows.append({
                'order_id': r['order_id'], 'source': r['source'], 'channel': r['channel'],
                'order_timestamp': r['order_timestamp'], 'time_of_day': r['time_of_day'],
                'vat_rate': 'Mixed', 'net_sales': net, 'tax': tax, 'gross_sales': gross
            })

    return pd.DataFrame(rows)
    
def process_ubereats(df):
    # Bulletproof check for Uber's double header:
    # If "Order ID" is sitting in the first row of data, push it up to be the actual columns
    first_row_vals = df.iloc[0].astype(str).str.strip().values
    if 'Order ID' in first_row_vals:
        df.columns = first_row_vals
        df = df.iloc[1:].copy()
        
    df.columns = df.columns.astype(str).str.strip()
    
    if 'Order status' not in df.columns:
        st.error(f"❌ Uber Eats format not recognized. Cannot find 'Order status'. Columns found: {', '.join(df.columns.tolist()[:5])}...")
        return pd.DataFrame()
        
    df = df[df['Order status'].astype(str).str.strip() == 'Completed'].copy()
    
    rows = []
    
    VAT_RATE_MAP = {
        'VAT 1 on sales': '6%',
        'VAT 2 on sales': '21%',
        'VAT 3 on sales': '12%'
    }
    
    for _, r in df.iterrows():
        order_val = r.get('Order ID') or r.iloc[0]
        order_id = 'UE_' + str(order_val)
        
        timestamp = pd.to_datetime(str(r.get('Order date', '')) + ' ' + str(r.get('Order confirmed time', '')), dayfirst=True, errors='coerce')
        time_of_day = get_time_of_day(timestamp.hour if pd.notnull(timestamp) else None)
        
        total_net = pd.to_numeric(r.get('Sales (excl. VAT)', 0), errors='coerce')
        total_gross = pd.to_numeric(r.get('Sales (incl. VAT)', 0), errors='coerce')
        
        found_tax = False
        
        for vat_col, rate_label in VAT_RATE_MAP.items():
            if vat_col not in r:
                continue
                
            tax_amount = pd.to_numeric(r.get(vat_col, 0), errors='coerce')
            if pd.isna(tax_amount) or tax_amount <= 0:
                continue
                
            found_tax = True
            numeric_rate = float(rate_label.replace('%', '')) / 100
            net_for_this_tax = round(tax_amount / numeric_rate, 2)
            gross_for_this_tax = round(net_for_this_tax + tax_amount, 2)
            
            rows.append({
                'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                'order_timestamp': timestamp, 'time_of_day': time_of_day,
                'vat_rate': rate_label, 'net_sales': net_for_this_tax, 'tax': tax_amount, 'gross_sales': gross_for_this_tax
            })
            
        if not found_tax:
            rows.append({
                'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                'order_timestamp': timestamp, 'time_of_day': time_of_day,
                'vat_rate': '0%', 'net_sales': total_net, 'tax': 0.0, 'gross_sales': total_gross
            })
            
    return pd.DataFrame(rows)

def process_deliveroo(df):
    df.columns = df.columns.astype(str).str.strip()

    if len(df.columns) == 1:
        df = df[df.columns[0]].astype(str).str.split(",", expand=True)
        df.columns = [
            'Restaurant name','Order number','Order status',
            'Date submitted','Time submitted',
            'Date delivered','Time delivered',
            'Subtotal','Deliveroo commission','VAT on Deliveroo commission'
        ]

    if 'Order status' in df.columns:
        df = df[df['Order status'].astype(str).str.strip() == 'Completed'].copy()
    else:
        st.error("❌ Deliveroo file format not recognized")
        return pd.DataFrame()

    df['order_id'] = 'DL_' + df['Order number'].astype(str)
    df['channel'] = 'Delivery'
    df['source'] = 'Deliveroo'

    df['order_timestamp'] = pd.to_datetime(
        df['Date submitted'].astype(str) + ' ' + df['Time submitted'].astype(str),
        errors='coerce',
        dayfirst=True
    )

    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['gross_sales'] = pd.to_numeric(df['Subtotal'], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'

    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']]

def process_takeaway(df):
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()
    
    df['order_id'] = 'TA_' + df['Order'].astype(str)
    
    if 'Pickup' in df.columns:
        df['channel'] = df['Pickup'].apply(lambda x: 'Takeaway' if str(x).strip().lower() == 'yes' else 'Delivery')
    else:
        df['channel'] = 'Delivery'
        
    df['source'] = 'Takeaway'
    df['order_timestamp'] = pd.to_datetime(df['Date'], dayfirst=True)
    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    
    if df['Total amount'].dtype == object:
        df['Total amount'] = df['Total amount'].str.replace(',', '.')
        
    df['gross_sales'] = pd.to_numeric(df['Total amount'], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'
    
    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']]

def save_to_db(clean_df):
    if clean_df.empty:
        return 0, 0

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO sales (order_id, source, channel, order_timestamp, time_of_day, vat_rate, net_sales, tax, gross_sales)
            VALUES (:order_id, :source, :channel, :order_timestamp, :time_of_day, :vat_rate, :net_sales, :tax, :gross_sales)
            ON CONFLICT (order_id, vat_rate) DO NOTHING
        """), clean_df.to_dict(orient="records"))
        inserted = result.rowcount

    skipped = len(clean_df) - inserted
    return inserted, skipped

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

if st.sidebar.button("Process File"):
    if uploaded_file:
        raw = uploaded_file.read()
        try:
            # Revert to safe parsing: try comma, then semicolon
            try:
                df_raw = pd.read_csv(io.BytesIO(raw), sep=',')
                if len(df_raw.columns) < 3:
                    df_raw = pd.read_csv(io.BytesIO(raw), sep=';')
            except:
                df_raw = pd.read_csv(io.BytesIO(raw), sep=';')
            
            parsers = {
                "Lightspeed K-Series": lambda d: process_lightspeed(d, "K-Series"),
                "Lightspeed L-Series": lambda d: process_lightspeed(d, "L-Series"),
                "Uber Eats": process_ubereats,
                "Deliveroo": process_deliveroo,
                "Takeaway": process_takeaway,
            }

            clean_df = parsers[source_option](df_raw)
            if clean_df.empty:
                st.sidebar.warning("No new or valid completed orders found in this file.")
            else:
                inserted, skipped = save_to_db(clean_df)
                st.sidebar.success(f"✅ Import completed: {inserted} rows added ({skipped} duplicates skipped).")
                st.cache_data.clear()
                st.rerun()
            
        except Exception as e:
            st.sidebar.error(f"Error parsing file: {e}")

data = load_data()

if not data.empty:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📅 Data currently in DB:**")
    summary_dates = data.groupby('source')['order_timestamp'].agg(['min', 'max']).reset_index()
    for _, row in summary_dates.iterrows():
        src = row['source']
        min_date = row['min'].date()
        max_date = row['max'].date()
        st.sidebar.caption(f"**{src}**\n{min_date} to {max_date}")
    st.sidebar.markdown("---")

st.sidebar.subheader("Maintenance")
if st.sidebar.button("🧹 Clean Database"):
    try:
        with engine.begin() as conn:
            count_before = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
            conn.execute(text("""
                DELETE FROM sales
                WHERE id NOT IN (
                    SELECT MIN(id) FROM sales GROUP BY order_id, vat_rate
                )
            """))
            count_after = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
            removed = count_before - count_after
            if removed > 0:
                st.sidebar.success(f"✅ Cleaned! Removed {removed} duplicate rows.")
            else:
                st.sidebar.info("Database is already clean! No duplicates found.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.sidebar.error(f"Error cleaning DB: {e}")

tab_dash, tab_vat = st.tabs(["Management Dashboard", "VAT Report"])

with tab_dash:
    st.header("Management Dashboard")
    if data.empty:
        st.info("No data yet — upload a CSV from the sidebar.")
    else:
        min_date_db = data['order_date'].min()
        max_date_db = data['order_date'].max()
        
        date_range = st.date_input("Filter Dashboard Date Range", [min_date_db, max_date_db], min_value=min_date_db, max_value=max_date_db)
        
        if len(date_range) == 2:
            dash_data = data[(data['order_date'] >= date_range[0]) & (data['order_date'] <= date_range[1])]
        else:
            dash_data = data
            
        total = dash_data['gross_sales'].sum()
        days = dash_data['order_date'].nunique() or 1
        lunch = dash_data[dash_data['time_of_day']=='Lunch']['gross_sales'].sum()
        dinner = dash_data[dash_data['time_of_day']=='Dinner']['gross_sales'].sum()

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
            trend = dash_data.groupby([grp_col,'year'])['gross_sales'].sum().reset_index()
            if not trend.empty:
                fig = px.bar(trend, x=grp_col, y='gross_sales', color='year', barmode='group')
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            by_channel = dash_data.groupby('channel')['gross_sales'].sum().reset_index()
            if not by_channel.empty:
                fig2 = px.pie(by_channel, values='gross_sales', names='channel', hole=0.4)
                st.plotly_chart(fig2, use_container_width=True)

        by_src = dash_data.groupby(['source','time_of_day'])['gross_sales'].sum().reset_index()
        if not by_src.empty:
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
        summary.columns = ['Source', 'VAT rate', 'Net', 'VAT', 'Gross']

        totals = pd.DataFrame([{
            'Source': 'TOTAL', 'VAT rate': '',
            'Net': summary['Net'].sum(),
            'VAT': summary['VAT'].sum(),
            'Gross': summary['Gross'].sum()
        }])
        
        st.dataframe(
            pd.concat([summary, totals], ignore_index=True)
              .style.format({'Net': '€{:.2f}', 'VAT': '€{:.2f}', 'Gross': '€{:.2f}'}),
            use_container_width=True
        )

        if st.button("Export to Excel"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                summary.to_excel(writer, sheet_name='Summary', index=False)
                q_data[['order_id','source','channel','order_timestamp',
                         'time_of_day','vat_rate','net_sales','tax','gross_sales']]\
                    .to_excel(writer, sheet_name='Transactions', index=False)
            st.download_button("Download Excel", output.getvalue(),
                file_name=f"VAT_{selected_q}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
