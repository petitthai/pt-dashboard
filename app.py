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
                gross_sales DOUBLE PRECISION
            )
        '''))
        # Regular index for fast duplicate pre-filtering — not unique
        conn.execute(text('''
            CREATE INDEX IF NOT EXISTS idx_sales_source_order_vat
                ON sales(source, order_id, vat_rate)
        '''))

def get_time_of_day(hour):
    if pd.isna(hour): return 'Unknown'
    return 'Lunch' if hour < 16 else 'Dinner'

def process_lightspeed(df, version="K-Series"):
    df.columns = df.columns.astype(str).str.strip()

    # ── Detect format: K-series (Identifier col) vs L-series (Receipt ID col) ──
    is_kseries = 'Identifier' in df.columns

    if is_kseries:
        # K-series: filter by Type=SALE and Canceled=No
        if 'Type' in df.columns:
            df = df[df['Type'].astype(str).str.strip() == 'SALE'].copy()
        if 'Canceled' in df.columns:
            df = df[df['Canceled'].astype(str).str.strip() == 'No'].copy()
        receipt_col = 'Identifier'
        tax_col     = 'TaxName'      # format: "BTW 12%:9.21|BTW 21%:2.78"
        tax_sep     = ':'            # separator between rate label and amount
        rate_prefix = 'BTW '        # prefix to strip to get numeric rate
    else:
        # L-series: filter by Status
        if 'Status' in df.columns:
            df = df[df['Status'].astype(str).str.strip().isin(['Paid', 'Done'])].copy()
        receipt_col = 'Receipt ID' if 'Receipt ID' in df.columns else df.columns[0]
        tax_col     = 'Taxes'        # format: "12%=1.5|21%=2.78"
        tax_sep     = '='
        rate_prefix = ''

    date_col = next(
        (c for c in ['Finalized Date', 'Creation Date', 'Date'] if c in df.columns),
        df.columns[1]
    )

    df['order_id']        = f'LS_{version}_' + df[receipt_col].astype(str)
    df['order_timestamp'] = pd.to_datetime(df[date_col], dayfirst=True)
    df['time_of_day']     = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['source']          = f'Lightspeed {version}'

    # Channel detection
    if 'Type' in df.columns and not is_kseries:
        df['channel'] = df['Type'].apply(
            lambda x: 'Takeaway' if str(x).lower() == 'takeaway' else 'In-Restaurant'
        )
    elif 'Mode' in df.columns:
        df['channel'] = df['Mode'].apply(
            lambda x: 'Takeaway' if str(x).lower() in ['takeout', 'takeaway', 'take-away'] else 'In-Restaurant'
        )
    else:
        df['channel'] = 'In-Restaurant'

    # ── VAT parsing ──────────────────────────────────────────────────────────
    has_tax = tax_col in df.columns
    has_vat = has_tax and df[tax_col].astype(str).str.contains(tax_sep, na=False).any()

    if has_vat:
        base = df[['order_id', 'source', 'channel', 'order_timestamp', 'time_of_day']].copy()
        base['vat_parts'] = df[tax_col].astype(str).str.split('|')
        exploded = base.explode('vat_parts')
        exploded = exploded[exploded['vat_parts'].str.contains(tax_sep, na=False)].copy()

        split = exploded['vat_parts'].str.strip().str.split(tax_sep, expand=True)

        # Extract numeric rate — strip "BTW " prefix and "%" suffix
        rate_str = split[0].str.replace(rate_prefix, '', regex=False).str.replace('%', '', regex=False).str.strip()
        exploded['rate'] = pd.to_numeric(rate_str, errors='coerce')
        exploded['tax']  = pd.to_numeric(split[1].str.strip(), errors='coerce')

        exploded = exploded.dropna(subset=['rate', 'tax'])
        exploded = exploded[(exploded['rate'] > 0) & (exploded['tax'] > 0)]

        exploded['net_sales']   = (exploded['tax'] / (exploded['rate'] / 100)).round(2)
        exploded['gross_sales'] = (exploded['net_sales'] + exploded['tax']).round(2)
        exploded['vat_rate']    = exploded['rate'].astype(int).astype(str) + '%'

        return exploded[['order_id', 'source', 'channel', 'order_timestamp', 'time_of_day',
                          'vat_rate', 'net_sales', 'tax', 'gross_sales']].reset_index(drop=True)

    else:
        # Fallback: no VAT breakdown available, use totals
        gross_col = next((c for c in ['Total', 'Total incl. Tax'] if c in df.columns), None)
        net_col   = next((c for c in ['PreTax', 'Net Total', 'Total excl. Tax'] if c in df.columns), None)
        df['gross_sales'] = pd.to_numeric(df[gross_col], errors='coerce').fillna(0) if gross_col else 0.0
        df['net_sales']   = pd.to_numeric(df[net_col],   errors='coerce').fillna(0) if net_col else 0.0
        df['tax']         = df['gross_sales'] - df['net_sales']
        df['vat_rate']    = 'Mixed'
        return df[['order_id', 'source', 'channel', 'order_timestamp', 'time_of_day',
                   'vat_rate', 'net_sales', 'tax', 'gross_sales']].reset_index(drop=True)
        
def process_ubereats(df):
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
    # Aggressively strip quotes and spaces to fix the 'Order' KeyError
    df.columns = df.columns.astype(str).str.strip().str.replace('"', '').str.replace("'", "")
    
    # Safely find columns ignoring case
    order_col = next((c for c in df.columns if 'order' in c.lower()), 'Order')
    pickup_col = next((c for c in df.columns if 'pickup' in c.lower()), 'Pickup')
    date_col = next((c for c in df.columns if 'date' in c.lower()), 'Date')
    total_col = next((c for c in df.columns if 'total amount' in c.lower() or 'total' in c.lower()), 'Total amount')

    df['order_id'] = 'TA_' + df[order_col].astype(str)
    
    if pickup_col in df.columns:
        df['channel'] = df[pickup_col].apply(lambda x: 'Takeaway' if str(x).strip().lower() == 'yes' else 'Delivery')
    else:
        df['channel'] = 'Delivery'
        
    df['source'] = 'Takeaway'
    df['order_timestamp'] = pd.to_datetime(df[date_col], errors='coerce')
    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    
    if df[total_col].dtype == object:
        df[total_col] = df[total_col].str.replace(',', '.')
        
    df['gross_sales'] = pd.to_numeric(df[total_col], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'
    
    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']]

# ✅ BLAZING FAST, VECTORIZED DB INSERT WITH PROGRESS TRACKING
def save_to_db_with_progress(clean_df, progress_bar=None):
    if clean_df.empty:
        return 0, 0

    # ── Step 1: fetch existing keys for this source ───────────────────────
    sources      = clean_df['source'].unique().tolist()
    placeholders = ', '.join(f':s{i}' for i in range(len(sources)))

    if progress_bar:
        progress_bar.progress(0.05, text="Checking database for existing records...")

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT order_id, vat_rate FROM sales WHERE source IN ({placeholders})"),
            {f's{i}': s for i, s in enumerate(sources)}
        )
        existing_df = pd.DataFrame(result.fetchall(), columns=['order_id', 'vat_rate'])

    # ── Step 2: vectorized dedup — no Python loop ─────────────────────────
    if not existing_df.empty:
        existing_keys = set(
            existing_df['order_id'].astype(str) + '||' + existing_df['vat_rate'].astype(str)
        )
    else:
        existing_keys = set()

    clean_df = clean_df.copy()
    clean_df['_key'] = clean_df['order_id'].astype(str) + '||' + clean_df['vat_rate'].astype(str)
    new_df   = clean_df[~clean_df['_key'].isin(existing_keys)].drop(columns=['_key']).copy()
    skipped  = len(clean_df) - len(new_df)

    if progress_bar:
        progress_bar.progress(0.2, text=f"Found {len(new_df)} new rows to insert ({skipped} duplicates skipped)...")

    if new_df.empty:
        return 0, skipped

    # ── Step 3: insert in chunks ──────────────────────────────────────────
    records    = new_df.to_dict(orient="records")
    chunk_size = 500
    total      = len(records)
    inserted   = 0

    for i in range(0, total, chunk_size):
        chunk = records[i : i + chunk_size]
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO sales
                    (order_id, source, channel, order_timestamp,
                     time_of_day, vat_rate, net_sales, tax, gross_sales)
                VALUES
                    (:order_id, :source, :channel, :order_timestamp,
                     :time_of_day, :vat_rate, :net_sales, :tax, :gross_sales)
            """), chunk)
            inserted += result.rowcount

        if progress_bar:
            progress_bar.progress(
                0.2 + 0.8 * min((i + chunk_size) / total, 1.0),
                text=f"Inserting... {min(i + chunk_size, total)}/{total} rows"
            )

    return inserted, skipped

@st.cache_data(ttl=60)
def load_data(full_history=False):
    if full_history:
        query = "SELECT * FROM sales ORDER BY order_timestamp"
    else:
        query = "SELECT * FROM sales WHERE order_timestamp >= now() - interval '36 months' ORDER BY order_timestamp"
        
    df = pd.read_sql(query, engine)
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

msg_placeholder = st.sidebar.empty()
if 'import_msg' in st.session_state:
    msg_placeholder.success(st.session_state.pop('import_msg'))

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
            df_raw = pd.read_csv(io.BytesIO(raw), sep=None, engine='python')
            
            parsers = {
                "Lightspeed K-Series": lambda d: process_lightspeed(d, "K-Series"),
                "Lightspeed L-Series": lambda d: process_lightspeed(d, "L-Series"),
                "Uber Eats": process_ubereats,
                "Deliveroo": process_deliveroo,
                "Takeaway": process_takeaway,
            }

            with st.spinner("Parsing file..."):
                clean_df = parsers[source_option](df_raw)

            if clean_df.empty:
                st.session_state['import_msg'] = "⚠️ No valid completed orders found in this file."
            else:
                progress = st.sidebar.progress(0, text="Checking database for duplicates...")
                inserted, skipped = save_to_db_with_progress(clean_df, progress)
                progress.empty()

                st.session_state['import_msg'] = (
                    f"✅ Import completed: {inserted} rows added, {skipped} duplicates skipped."
                )
                
            st.cache_data.clear()
            st.rerun()
            
        except Exception as e:
            st.sidebar.error(f"Error parsing file: {e}")

data = load_data(full_history=False)

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
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER(PARTITION BY order_id, vat_rate ORDER BY id) as row_num
                        FROM sales
                    ) t WHERE t.row_num > 1
                )
            """))
            count_after = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
            removed = count_before - count_after
            if removed > 0:
                st.session_state['import_msg'] = f"✅ Cleaned! Removed {removed} duplicate rows."
            else:
                st.session_state['import_msg'] = "Database is already clean! No duplicates found."
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
        
        if len(date_range) != 2:
            st.warning("Please select both a start and end date to view the dashboard.")
            st.stop()
            
        dash_data = data[(data['order_date'] >= date_range[0]) & (data['order_date'] <= date_range[1])]
            
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
    
    vat_data = load_data(full_history=True)
    
    if vat_data.empty:
        st.info("No data yet.")
    else:
        quarters = sorted(vat_data['quarter'].unique(), reverse=True)
        selected_q = st.selectbox("Quarter", quarters)
        q_data = vat_data[vat_data['quarter'] == selected_q]

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
