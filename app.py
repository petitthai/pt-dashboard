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
        conn.execute(text('''
            ALTER TABLE sales ADD COLUMN IF NOT EXISTS commission_ex_vat DOUBLE PRECISION DEFAULT 0.0;
        '''))
        conn.execute(text('''
            CREATE INDEX IF NOT EXISTS idx_sales_source_order_vat
                ON sales(source, order_id, vat_rate)
        '''))

def get_time_of_day(hour):
    if pd.isna(hour): return 'Unknown'
    return 'Lunch' if hour < 16 else 'Dinner'

def process_lightspeed(df, version="K-Series"):
    df.columns = df.columns.astype(str).str.strip()

    is_kseries = 'Identifier' in df.columns
    version = "K-Series" if is_kseries else "L-Series"

    if is_kseries:
        if 'Type' in df.columns:
            df = df[df['Type'].astype(str).str.strip() == 'SALE'].copy()
        if 'Canceled' in df.columns:
            df = df[df['Canceled'].astype(str).str.strip() == 'No'].copy()
        receipt_col = 'Identifier'
        tax_col     = 'TaxName'      
        tax_sep     = ':'            
        rate_prefix = 'BTW '        
    else:
        if 'Status' in df.columns:
            df = df[df['Status'].astype(str).str.strip().isin(['Paid', 'Done'])].copy()
        receipt_col = 'Receipt ID' if 'Receipt ID' in df.columns else df.columns[0]
        tax_col     = 'Taxes'        
        tax_sep     = '='
        rate_prefix = ''

    date_col = next(
        (c for c in ['Finalized Date', 'Creation Date', 'Date'] if c in df.columns),
        df.columns[1]
    )
    gross_col = next((c for c in ['Total','Total incl. Tax'] if c in df.columns), None)

    df['order_id']        = f'LS_{version}_' + df[receipt_col].astype(str)
    df['channel']         = df['Type'].apply(lambda x: 'Takeaway' if str(x).lower()=='takeaway' else 'In-Restaurant') if 'Type' in df.columns else 'In-Restaurant'
    df['order_timestamp'] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
    df['time_of_day']     = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['source']          = f'Lightspeed {version}'
    df['receipt_total']   = pd.to_numeric(df[gross_col], errors='coerce').fillna(0) if gross_col else 0.0

    has_tax_col = tax_col in df.columns
    has_vat     = has_tax_col and df[tax_col].astype(str).str.contains(tax_sep).any()

    if has_vat:
        base = df[['order_id','source','channel','order_timestamp','time_of_day', 'receipt_total']].copy()
        
        vat_df = base.copy()
        vat_df['vat_parts'] = df[tax_col].astype(str).str.split('|')
        exploded = vat_df.explode('vat_parts')
        exploded = exploded[exploded['vat_parts'].str.contains(tax_sep, na=False)].copy()

        split = exploded['vat_parts'].str.strip().str.split(tax_sep, expand=True)
        exploded['rate']     = pd.to_numeric(split[0].str.replace(rate_prefix, '', regex=False).str.replace('%','').str.strip(), errors='coerce')
        exploded['tax']      = pd.to_numeric(split[1].str.strip(), errors='coerce')
        
        exploded = exploded.dropna(subset=['rate','tax'])
        exploded = exploded[exploded['rate'] > 0] 

        exploded['net_sales']   = (exploded['tax'] / (exploded['rate'] / 100)).round(2)
        exploded['gross_sales'] = (exploded['net_sales'] + exploded['tax']).round(2)
        exploded['vat_rate']    = exploded['rate'].astype(int).astype(str) + '%'
        exploded['commission_ex_vat'] = 0.0

        valid_vat_rows = exploded[['order_id','source','channel','order_timestamp','time_of_day',
                          'vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]

        calc_sums = valid_vat_rows.groupby('order_id')['gross_sales'].sum().reset_index(name='calc_gross')
        base = base.merge(calc_sums, on='order_id', how='left')
        base['calc_gross'] = base['calc_gross'].fillna(0)
        base['remainder'] = (base['receipt_total'] - base['calc_gross']).round(2)

        zero_vat_rows = base[base['remainder'] >= 0.01].copy()
        if not zero_vat_rows.empty:
            zero_vat_rows['vat_rate'] = '0%'
            zero_vat_rows['net_sales'] = zero_vat_rows['remainder']
            zero_vat_rows['tax'] = 0.0
            zero_vat_rows['gross_sales'] = zero_vat_rows['remainder']
            zero_vat_rows['commission_ex_vat'] = 0.0
            
            zero_vat_rows = zero_vat_rows[['order_id','source','channel','order_timestamp','time_of_day',
                                           'vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]
            
            final_df = pd.concat([valid_vat_rows, zero_vat_rows], ignore_index=True)
        else:
            final_df = valid_vat_rows

        return final_df.reset_index(drop=True)
    else:
        net_col   = next((c for c in ['Net Total','Total excl. Tax'] if c in df.columns), None)
        df['gross_sales'] = df['receipt_total']
        df['net_sales']   = pd.to_numeric(df[net_col],   errors='coerce').fillna(0) if net_col else 0.0
        df['tax']         = df['gross_sales'] - df['net_sales']
        df['vat_rate']    = 'Mixed'
        df['commission_ex_vat'] = 0.0
        return df[['order_id','source','channel','order_timestamp','time_of_day',
                   'vat_rate','net_sales','tax','gross_sales','commission_ex_vat']].reset_index(drop=True)
    
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
    
    ue_fee_cols = [
        'Marketplace fee after discount (excl. VAT)',
        'Cost of delivery (excl. VAT)',
        'Offers on items (excl. VAT)',
        'Order error adjustments (excl. VAT)'
    ]
    
    for _, r in df.iterrows():
        order_val = str(r.get('Order ID') or r.iloc[0]).strip()
        date_str = str(r.get('Order date', '')).strip()
        time_str = str(r.get('Order confirmed time', '')).strip()
        
        timestamp = pd.to_datetime(date_str + ' ' + time_str, dayfirst=True, errors='coerce')
        time_of_day = get_time_of_day(timestamp.hour if pd.notnull(timestamp) else None)
        
        date_suffix = timestamp.strftime('%Y%m%d') if pd.notnull(timestamp) else "Unknown"
        order_id = f"UE_{order_val}_{date_suffix}"
        
        total_net = pd.to_numeric(r.get('Sales (excl. VAT)', 0), errors='coerce')
        total_gross = pd.to_numeric(r.get('Sales (incl. VAT)', 0), errors='coerce')
        
        vat1 = pd.to_numeric(r.get('VAT 1 on sales', 0), errors='coerce')
        vat2 = pd.to_numeric(r.get('VAT 2 on sales', 0), errors='coerce')
        vat3 = pd.to_numeric(r.get('VAT 3 on sales', 0), errors='coerce')
        
        vat1 = 0 if pd.isna(vat1) else vat1
        vat2 = 0 if pd.isna(vat2) else vat2
        vat3 = 0 if pd.isna(vat3) else vat3
        
        active_vats = sum([vat1 > 0, vat2 > 0, vat3 > 0])
        
        total_comm = 0.0
        for fc in ue_fee_cols:
            val = pd.to_numeric(r.get(fc, 0), errors='coerce')
            if pd.notna(val):
                total_comm -= val 
        
        if active_vats == 1:
            if vat1 > 0:
                rate_label, tax_amount = '6%', vat1
            elif vat2 > 0:
                rate_label, tax_amount = '21%', vat2
            else:
                rate_label, tax_amount = '12%', vat3
                
            rows.append({
                'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                'order_timestamp': timestamp, 'time_of_day': time_of_day,
                'vat_rate': rate_label, 'net_sales': total_net, 'tax': tax_amount, 
                'gross_sales': total_gross, 'commission_ex_vat': round(total_comm, 2)
            })
            
        elif active_vats > 1:
            VAT_MAP = [('6%', vat1), ('21%', vat2), ('12%', vat3)]
            for label, amt in VAT_MAP:
                if amt > 0:
                    numeric_rate = float(label.replace('%', '')) / 100
                    net = round(amt / numeric_rate, 2)
                    gross = round(net + amt, 2)
                    
                    ratio = gross / total_gross if total_gross > 0 else 0
                    comm_split = round(total_comm * ratio, 2)
                    
                    rows.append({
                        'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                        'order_timestamp': timestamp, 'time_of_day': time_of_day,
                        'vat_rate': label, 'net_sales': net, 'tax': amt, 
                        'gross_sales': gross, 'commission_ex_vat': comm_split
                    })
        else:
            rows.append({
                'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                'order_timestamp': timestamp, 'time_of_day': time_of_day,
                'vat_rate': '0%', 'net_sales': total_net, 'tax': 0.0, 
                'gross_sales': total_gross, 'commission_ex_vat': round(total_comm, 2)
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

    df['channel'] = 'Delivery'
    df['source'] = 'Deliveroo'

    df['order_timestamp'] = pd.to_datetime(
        df['Date submitted'].astype(str) + ' ' + df['Time submitted'].astype(str),
        errors='coerce',
        dayfirst=True
    )

    date_str = df['order_timestamp'].dt.strftime('%Y%m%d').fillna('Unknown')
    df['order_id'] = 'DL_' + df['Order number'].astype(str) + '_' + date_str

    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['gross_sales'] = pd.to_numeric(df['Subtotal'], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'
    
    df['commission_ex_vat'] = pd.to_numeric(df['Deliveroo commission'], errors='coerce').fillna(0).abs()

    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]

def process_takeaway(df):
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip().str.replace('"', '').str.replace("'", "")
    
    order_col = next((c for c in df.columns if 'order' in c.lower()), 'Order')
    pickup_col = next((c for c in df.columns if 'pickup' in c.lower()), 'Pickup')
    date_col = next((c for c in df.columns if 'date' in c.lower()), 'Date')
    total_col = next((c for c in df.columns if 'total amount' in c.lower() or 'total' in c.lower()), 'Total amount')

    df['order_timestamp'] = pd.to_datetime(df[date_col], errors='coerce')
    
    date_str = df['order_timestamp'].dt.strftime('%Y%m%d').fillna('Unknown')
    df['order_id'] = 'TA_' + df[order_col].astype(str) + '_' + date_str
    
    if pickup_col in df.columns:
        df['channel'] = df[pickup_col].apply(lambda x: 'Takeaway' if str(x).strip().lower() == 'yes' else 'Delivery')
    else:
        df['channel'] = 'Delivery'
        
    df['source'] = 'Takeaway'
    df['time_of_day'] = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    
    if df[total_col].dtype == object:
        df[total_col] = df[total_col].str.replace(',', '.')
        
    df['gross_sales'] = pd.to_numeric(df[total_col], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'
    
    df['commission_ex_vat'] = (df['gross_sales'] * 0.30).round(2)
    
    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]

def save_to_db_with_progress(clean_df, progress_bar=None):
    if clean_df.empty:
        return 0, 0

    if progress_bar:
        progress_bar.progress(0.05, text="Checking database for existing records...")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT order_id, vat_rate FROM sales"))
        existing_df = pd.DataFrame(result.fetchall(), columns=['order_id', 'vat_rate'])

    if not existing_df.empty:
        existing_df['univ_order_id'] = existing_df['order_id'].str.replace('LS_K-Series_', 'LS_').str.replace('LS_L-Series_', 'LS_')
        existing_keys = set(existing_df['univ_order_id'].astype(str) + '||' + existing_df['vat_rate'].astype(str).str.strip())
    else:
        existing_keys = set()

    clean_df = clean_df.copy()
    clean_df['univ_order_id'] = clean_df['order_id'].str.replace('LS_K-Series_', 'LS_').str.replace('LS_L-Series_', 'LS_')
    clean_df['_key'] = clean_df['univ_order_id'].astype(str) + '||' + clean_df['vat_rate'].astype(str).str.strip()
    
    new_df = clean_df[~clean_df['_key'].isin(existing_keys)].drop(columns=['_key', 'univ_order_id']).copy()
    skipped  = len(clean_df) - len(new_df)

    if progress_bar:
        progress_bar.progress(0.2, text=f"Found {len(new_df)} new rows to insert ({skipped} duplicates skipped)...")

    if new_df.empty:
        return 0, skipped

    # ✅ ANTI-CRASH FIX: Vervang NaN/NaT door None, anders crasht PostgreSQL (DataError)!
    new_df = new_df.astype(object).where(pd.notna(new_df), None)

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
                     time_of_day, vat_rate, net_sales, tax, gross_sales, commission_ex_vat)
                VALUES
                    (:order_id, :source, :channel, :order_timestamp,
                     :time_of_day, :vat_rate, :net_sales, :tax, :gross_sales, :commission_ex_vat)
            """), chunk)
            inserted += result.rowcount

        if progress_bar:
            progress_bar.progress(
                0.2 + 0.8 * min((i + chunk_size) / total, 1.0),
                text=f"Updating database... {min(i + chunk_size, total)}/{total} rows added"
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
        
        if 'commission_ex_vat' in df.columns:
            df['commission_ex_vat'] = pd.to_numeric(df['commission_ex_vat'], errors='coerce').fillna(0.0)
            
    return df

# --- APP ---
st.set_page_config(page_title="Restaurant OS", layout="wide")
init_db()

st.sidebar.title("Data Sync")

msg_placeholder = st.sidebar.empty()
if 'import_msg' in st.session_state:
    msg_placeholder.success(st.session_state.pop('import_msg'))

with st.sidebar.expander("How to update?", expanded=True):
    st.markdown("""
- **K-Series:** Backoffice → Reports → Receipts → Export CSV  
- **Uber Eats:** UE Manager → Payments → Invoices → Export CSV  
- **Deliveroo:** Hub → Invoices → Orders → Export CSV  
- **Takeaway:** Portal → Invoicing → Orders → Export CSV
    """)

source_option = st.sidebar.selectbox("Source", ["Lightspeed K-Series","Lightspeed L-Series","Deliveroo","Uber Eats","Takeaway"])
uploaded_files = st.sidebar.file_uploader("Upload CSV", type=["csv"], accept_multiple_files=True)

if st.sidebar.button("Process File(s)"):
    if uploaded_files:
        all_clean_dfs = []
        
        with st.spinner("Parsing files..."):
            for file in uploaded_files:
                raw = file.read()
                try:
                    df_raw = pd.read_csv(io.BytesIO(raw), sep=None, engine='python')
                    
                    parsers = {
                        "Lightspeed K-Series": lambda d: process_lightspeed(d, "K-Series"),
                        "Lightspeed L-Series": lambda d: process_lightspeed(d, "L-Series"),
                        "Uber Eats": process_ubereats,
                        "Deliveroo": process_deliveroo,
                        "Takeaway": process_takeaway,
                    }
                    
                    clean_df = parsers[source_option](df_raw)
                    if not clean_df.empty:
                        all_clean_dfs.append(clean_df)
                except Exception as e:
                    st.sidebar.error(f"Error parsing file {file.name}: {e}")

        if not all_clean_dfs:
            st.session_state['import_msg'] = "⚠️ No valid or completed orders found in these files."
        else:
            combined_df = pd.concat(all_clean_dfs, ignore_index=True)
            progress = st.sidebar.progress(0, text="Checking database for duplicates...")
            inserted, skipped = save_to_db_with_progress(combined_df, progress)
            progress.empty()

            st.session_state['import_msg'] = (
                f"✅ Import successful: {inserted} new rows added, {skipped} duplicates skipped."
            )
            
        st.cache_data.clear()
        st.rerun()

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
    with st.spinner("Cleaning database thoroughly..."):
        try:
            with engine.begin() as conn:
                result_fout = conn.execute(text("""
                    DELETE FROM sales 
                    WHERE source = 'Lightspeed L-Series' 
                    AND order_id LIKE 'LS_L-Series_R%'
                """))
                foute_verwijderd = result_fout.rowcount

                count_before = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
                
                conn.execute(text("""
                    DELETE FROM sales
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER(
                                PARTITION BY 
                                    REPLACE(REPLACE(order_id, 'LS_K-Series_', 'LS_'), 'LS_L-Series_', 'LS_'), 
                                    vat_rate 
                                ORDER BY 
                                    CASE WHEN source = 'Lightspeed K-Series' THEN 1 ELSE 2 END,
                                    id ASC
                            ) as row_num
                            FROM sales
                        ) t WHERE t.row_num > 1
                    )
                """))
                count_after = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
                dubbels_verwijderd = count_before - count_after
                
                totaal_verwijderd = foute_verwijderd + dubbels_verwijderd
                
                if totaal_verwijderd > 0:
                    st.session_state['import_msg'] = f"✅ Cleaned! {foute_verwijderd} invalid L-Series removed and {dubbels_verwijderd} duplicate rows deleted."
                else:
                    st.session_state['import_msg'] = "Database is already clean! No invalid formats or duplicates found."
                    
            st.cache_data.clear()
            st.rerun()
            
        except Exception as e:
            st.sidebar.error(f"Error cleaning DB: {e}")

tab_dash, tab_vat = st.tabs(["Management Dashboard", "VAT Report"])

with tab_dash:
    st.header("Management Dashboard")
    if data.empty:
        st.info("No data in the system yet — upload a CSV via the sidebar.")
    else:
        min_date_db = data['order_date'].min()
        max_date_db = data['order_date'].max()
        
        date_range = st.date_input("Filter Dashboard Date Range", [min_date_db, max_date_db], min_value=min_date_db, max_value=max_date_db)
        
        if len(date_range) != 2:
            st.warning("Please select a start and end date to view the dashboard.")
            st.stop()
            
        dash_data = data[(data['order_date'] >= date_range[0]) & (data['order_date'] <= date_range[1])]
            
        total = dash_data['gross_sales'].sum()
        days = dash_data['order_date'].nunique() or 1
        lunch = dash_data[dash_data['time_of_day']=='Lunch']['gross_sales'].sum()
        dinner = dash_data[dash_data['time_of_day']=='Dinner']['gross_sales'].sum()

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Revenue", f"€{total:,.2f}")
        c2.metric("Avg per open day", f"€{total/days:,.2f}")
        c3.metric("Lunch", f"€{lunch:,.2f}", f"{lunch/total*100:.1f}%" if total else "")
        c4.metric("Dinner", f"€{dinner:,.2f}", f"{dinner/total*100:.1f}%" if total else "")

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            view = st.radio("View", ["Weekly","Monthly"], horizontal=True)
            grp_col = 'week_str' if view == 'Weekly' else 'month'
            trend = dash_data.groupby([grp_col,'year'])['gross_sales'].sum().reset_index()
            if not trend.empty:
                fig = px.bar(trend, x=grp_col, y='gross_sales', color='year', barmode='group', title="Revenue Trend")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            by_channel = dash_data.groupby('channel')['gross_sales'].sum().reset_index()
            if not by_channel.empty:
                fig2 = px.pie(by_channel, values='gross_sales', names='channel', hole=0.4, title="Revenue by Channel")
                st.plotly_chart(fig2, use_container_width=True)

        by_src = dash_data.groupby(['source','time_of_day'])['gross_sales'].sum().reset_index()
        if not by_src.empty:
            fig3 = px.bar(by_src, x='source', y='gross_sales', color='time_of_day', title="Revenue by Source & Time of Day")
            st.plotly_chart(fig3, use_container_width=True)

with tab_vat:
    st.header("VAT Report")
    
    vat_data = load_data(full_history=True)
    
    if vat_data.empty:
        st.info("No data in the report yet.")
    else:
        quarters = sorted(vat_data['quarter'].unique(), reverse=True)
        selected_q = st.selectbox("Select Quarter", quarters)
        q_data = vat_data[vat_data['quarter'] == selected_q]

        if 'commission_ex_vat' in q_data.columns:
            summary = q_data.groupby(['source','vat_rate'])[['net_sales','tax','gross_sales', 'commission_ex_vat']].sum().reset_index()
            summary.columns = ['Source', 'VAT Rate', 'Net', 'VAT', 'Gross', 'Commission (ex VAT)']
        else:
            summary = q_data.groupby(['source','vat_rate'])[['net_sales','tax','gross_sales']].sum().reset_index()
            summary.columns = ['Source', 'VAT Rate', 'Net', 'VAT', 'Gross']

        totals_dict = {
            'Source': 'TOTAL', 'VAT Rate': '',
            'Net': summary['Net'].sum(),
            'VAT': summary['VAT'].sum(),
            'Gross': summary['Gross'].sum()
        }
        if 'Commission (ex VAT)' in summary.columns:
            totals_dict['Commission (ex VAT)'] = summary['Commission (ex VAT)'].sum()
            
        totals = pd.DataFrame([totals_dict])
        
        st.subheader("Overview by Source")
        
        format_dict = {'Net': '€{:.2f}', 'VAT': '€{:.2f}', 'Gross': '€{:.2f}'}
        if 'Commission (ex VAT)' in summary.columns:
            format_dict['Commission (ex VAT)'] = '€{:.2f}'
            
        st.dataframe(
            pd.concat([summary, totals], ignore_index=True)
              .style.format(format_dict),
            use_container_width=True
        )
        
        with st.expander("Show all individual transactions (including Order ID)"):
            cols_to_show = ['order_id', 'source', 'channel', 'order_timestamp', 'time_of_day', 'vat_rate', 'net_sales', 'tax', 'gross_sales']
            if 'commission_ex_vat' in q_data.columns:
                cols_to_show.append('commission_ex_vat')
            st.dataframe(
                q_data[cols_to_show],
                use_container_width=True
            )

        if st.button("Export to Excel"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                summary.to_excel(writer, sheet_name='1. Summary', index=False)
                
                cols_to_export = ['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales']
                if 'commission_ex_vat' in q_data.columns:
                    cols_to_export.append('commission_ex_vat')
                    
                q_data[cols_to_export].to_excel(writer, sheet_name='2. All_Transactions_Details', index=False)
                    
            st.download_button("📥 Download Excel File", output.getvalue(),
                file_name=f"VAT_Report_{selected_q}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
