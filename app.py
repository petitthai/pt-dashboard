import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import plotly.express as px
import io

DB_URL = st.secrets["SUPABASE_URL"]
engine = create_engine(DB_URL, connect_args={"sslmode": "require"}, pool_pre_ping=True)

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
        conn.execute(text('''ALTER TABLE sales ADD COLUMN IF NOT EXISTS commission_ex_vat DOUBLE PRECISION DEFAULT 0.0;'''))
        conn.execute(text('''CREATE INDEX IF NOT EXISTS idx_sales_source_order_vat ON sales(source, order_id, vat_rate)'''))

def process_lightspeed(df):
    df.columns = df.columns.astype(str).str.strip()
    is_kseries = 'Identifier' in df.columns
    version = "K-Series" if is_kseries else "L-Series"

    if is_kseries:
        if 'Type' in df.columns:
            df = df[df['Type'].astype(str).str.strip().isin(['SALE', 'Sale', 'Verkoop', 'Vente'])].copy()
        if 'Canceled' in df.columns:
            df = df[df['Canceled'].astype(str).str.strip().isin(['No', 'Nee', 'Non', 'False'])].copy()
        receipt_col, tax_col, tax_sep, rate_prefix = 'Identifier', 'TaxName', ':', 'BTW '        
        vat_sep = '|' # Scheidingsteken voor K-Series BTW
    else:
        if 'Status' in df.columns:
            df = df[df['Status'].astype(str).str.strip().isin(['Paid', 'Done', 'Betaald', 'Afgerekend', 'Payé', 'Terminé', 'Voltooid'])].copy()
        receipt_col = 'Receipt ID' if 'Receipt ID' in df.columns else df.columns[0]
        tax_col, tax_sep, rate_prefix = 'Taxes', '=', ''
        vat_sep = '|' # Scheidingsteken voor L-Series BTW

    if df.empty or receipt_col not in df.columns: return pd.DataFrame()
    df = df.dropna(subset=[receipt_col])
    df = df.drop_duplicates(subset=[receipt_col]).copy()

    date_col = next((c for c in ['Finalized Date', 'Creation Date', 'Date'] if c in df.columns), df.columns[1])
    gross_col = next((c for c in ['Total','Total incl. Tax'] if c in df.columns), None)

    df['order_id'] = f'LS_{version}_' + df[receipt_col].astype(str)
    
    if is_kseries:
        profile_col = df.get('Profile', pd.Series(dtype=str, index=df.index))
        df['channel'] = profile_col.apply(
            lambda x: 'Takeaway' if str(x).strip().lower() in
            ['takeout', 'takeaway', 'take-away', 'take away', 'oa take-away', 'afhaal', 'emporter']
            else 'In-Restaurant'
        )
    else:
        df['channel'] = df.get('Type', pd.Series(dtype=str)).apply(lambda x: 'Takeaway' if str(x).lower().strip() in ['takeaway', 'afhaal', 'emporter'] else 'In-Restaurant')
            
    df['order_timestamp'] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
    df['time_of_day'] = 'Lunch'
    df.loc[df['order_timestamp'].dt.hour >= 16, 'time_of_day'] = 'Dinner'
    df.loc[df['order_timestamp'].isna(), 'time_of_day'] = 'Unknown'

    df['source'] = f'Lightspeed {version}'
    df['receipt_total'] = pd.to_numeric(df[gross_col], errors='coerce').fillna(0) if gross_col else 0.0

    if tax_col in df.columns and df[tax_col].astype(str).str.contains(tax_sep).any():
        base = df[['order_id','source','channel','order_timestamp','time_of_day', 'receipt_total', tax_col]].copy()
        vat_df = base.copy()
        
        vat_df['vat_parts'] = vat_df[tax_col].astype(str).str.split(vat_sep)
        exploded = vat_df.explode('vat_parts')
        
        exploded = exploded[exploded['vat_parts'].str.contains(tax_sep, na=False)].copy()
        split = exploded['vat_parts'].str.strip().str.split(tax_sep, expand=True)
        
        rate_str = split[0]
        if rate_prefix:
            rate_str = rate_str.str.replace(rate_prefix, '', regex=False)
            
        exploded['rate'] = pd.to_numeric(rate_str.str.replace('%', '', regex=False).str.strip(), errors='coerce')
        exploded['tax'] = pd.to_numeric(split[1].str.strip(), errors='coerce')
        
        exploded = exploded.dropna(subset=['rate','tax'])
        exploded = exploded[exploded['rate'] > 0] 

        exploded['net_sales'] = (exploded['tax'] / (exploded['rate'] / 100)).round(2)
        exploded['gross_sales'] = (exploded['net_sales'] + exploded['tax']).round(2)
        exploded['vat_rate'] = exploded['rate'].astype(int).astype(str) + '%'
        exploded['commission_ex_vat'] = 0.0

        valid_vat = exploded[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]
        calc_sums = valid_vat.groupby('order_id')['gross_sales'].sum().reset_index(name='calc_gross')
        base = base.merge(calc_sums, on='order_id', how='left')
        base['calc_gross'] = base['calc_gross'].fillna(0)
        base['remainder'] = (base['receipt_total'] - base['calc_gross']).round(2)

        zero_vat = base[base['remainder'] >= 0.01].copy()
        if not zero_vat.empty:
            zero_vat['vat_rate'], zero_vat['tax'], zero_vat['commission_ex_vat'] = '0%', 0.0, 0.0
            zero_vat['net_sales'] = zero_vat['gross_sales'] = zero_vat['remainder']
            zero_vat = zero_vat[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]
            return pd.concat([valid_vat, zero_vat], ignore_index=True).reset_index(drop=True)
        return valid_vat.reset_index(drop=True)
    else:
        net_col = next((c for c in ['Net Total','Total excl. Tax'] if c in df.columns), None)
        df['gross_sales'] = df['receipt_total']
        df['net_sales'] = pd.to_numeric(df[net_col], errors='coerce').fillna(0) if net_col else 0.0
        df['tax'] = df['gross_sales'] - df['net_sales']
        df['vat_rate'], df['commission_ex_vat'] = 'Mixed', 0.0
        return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']].reset_index(drop=True)

def process_ubereats(df):
    # Promote the short-code row to be the real header
    if 'Order ID' in df.iloc[0].astype(str).str.strip().values:
        df.columns = df.iloc[0].astype(str).str.strip().values
        df = df.iloc[1:].copy()
    df.columns = df.columns.astype(str).str.strip()

    # --- Structure validation: fail loudly instead of silently defaulting to 0 ---
    required_cols = [
        'Order status',
        'Order ID',
        'Order date',
        'Order confirmed time',
        'VAT 1 on sales', 'VAT 2 on sales', 'VAT 3 on sales',
        'VAT 1 on order error adjustments', 'VAT 2 on order error adjustments', 'VAT 3 on order error adjustments',
        'VAT 1 on offers on items', 'VAT 2 on offers on items', 'VAT 3 on offers on items',
        'Marketplace Fee after promotion (excl. VAT)',
        'Cost of delivery (excl. VAT)',
        'Order error adjustments (excl. VAT)',
        'Offer Redemption Fee',
        'Marketing adjustment (incl. VAT)',
        'Total order (incl. VAT)',
        'Sales (incl. VAT)',
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            "Uber Eats file structure has changed — import aborted.\n"
            f"Missing expected column(s): {', '.join(missing)}\n"
            "No rows from this file were imported. Please check the export "
            "format or update the column mapping in process_ubereats()."
        )

    df = df[df['Order status'].astype(str).str.strip() == 'Completed'].copy()

    def num(r, col):
        v = pd.to_numeric(r.get(col, 0), errors='coerce')
        return 0.0 if pd.isna(v) else float(v)

    rate_pct = {'6%': 0.06, '12%': 0.12, '21%': 0.21}
    rows = []

    for _, r in df.iterrows():
        order_val = str(r.get('Order ID') or r.iloc[0]).strip()
        ts = pd.to_datetime(
            str(r.get('Order date', '')).strip() + ' ' + str(r.get('Order confirmed time', '')).strip(),
            dayfirst=True, errors='coerce'
        )
        tod = 'Lunch' if pd.notnull(ts) and ts.hour < 16 else ('Dinner' if pd.notnull(ts) else 'Unknown')
        oid = f"UE_{order_val}_{ts.strftime('%Y%m%d') if pd.notnull(ts) else 'Unknown'}"
        base_row = {'order_id': oid, 'source': 'Uber Eats', 'channel': 'Delivery',
                    'order_timestamp': ts, 'time_of_day': tod}

        # Commission: fee columns are stored negative -> flip sign for a positive cost
        fee_service = num(r, 'Marketplace Fee after promotion (excl. VAT)')
        fee_delivery = num(r, 'Cost of delivery (excl. VAT)')
        fee_error = num(r, 'Order error adjustments (excl. VAT)')
        fee_redemp = num(r, 'Offer Redemption Fee')
        adj_marketing = num(r, 'Marketing adjustment (incl. VAT)')

        t_comm = abs(fee_service) + abs(fee_delivery) + abs(fee_error) + abs(fee_redemp)
        t_comm -= abs(adj_marketing)

        # Net VAT per rate = sales VAT + error-adjustment VAT + offer VAT
        # (offers/adjustments are already negative in the CSV, so summing nets them out)
        v1_total = num(r, 'VAT 1 on sales') + num(r, 'VAT 1 on order error adjustments') + num(r, 'VAT 1 on offers on items')
        v2_total = num(r, 'VAT 2 on sales') + num(r, 'VAT 2 on order error adjustments') + num(r, 'VAT 2 on offers on items')
        v3_total = num(r, 'VAT 3 on sales') + num(r, 'VAT 3 on order error adjustments') + num(r, 'VAT 3 on offers on items')

        active = []
        if abs(v1_total) > 0.001: active.append(('6%', abs(v1_total)))
        if abs(v2_total) > 0.001: active.append(('12%', abs(v2_total)))
        if abs(v3_total) > 0.001: active.append(('21%', abs(v3_total)))

        if not active:
            t_gross_total = num(r, 'Total order (incl. VAT)') or num(r, 'Sales (incl. VAT)')
            rows.append({**base_row, 'vat_rate': '0%', 'net_sales': t_gross_total,
                         'tax': 0.0, 'gross_sales': t_gross_total,
                         'commission_ex_vat': round(t_comm, 2)})
            continue

        computed = []
        gross_sum = 0.0
        for lbl, vat_amt in active:
            net_amt = round(vat_amt / rate_pct[lbl], 2)
            gross_amt = round(net_amt + vat_amt, 2)
            computed.append((lbl, net_amt, vat_amt, gross_amt))
            gross_sum += gross_amt

        for i, (lbl, net_amt, vat_amt, gross_amt) in enumerate(computed):
            if len(computed) > 1 and gross_sum:
                row_comm = round(t_comm * (gross_amt / gross_sum), 2)
            else:
                row_comm = round(t_comm, 2) if i == 0 else 0.0
            rows.append({**base_row, 'vat_rate': lbl, 'net_sales': net_amt,
                         'tax': round(vat_amt, 2), 'gross_sales': gross_amt,
                         'commission_ex_vat': row_comm})

    return pd.DataFrame(rows)
    
def process_deliveroo(df):
    df.columns = df.columns.astype(str).str.strip()
    if len(df.columns) == 1:
        df = df[df.columns[0]].astype(str).str.split(",", expand=True)
        df.columns = ['Restaurant name','Order number','Order status','Date submitted','Time submitted','Date delivered','Time delivered','Subtotal','Deliveroo commission','VAT on Deliveroo commission']

    if 'Order status' not in df.columns: return pd.DataFrame()
    df = df[df['Order status'].astype(str).str.strip() == 'Completed'].copy()

    df['channel'], df['source'] = 'Delivery', 'Deliveroo'
    df['order_timestamp'] = pd.to_datetime(df['Date submitted'].astype(str) + ' ' + df['Time submitted'].astype(str), errors='coerce')
    df['order_id'] = 'DL_' + df['Order number'].astype(str) + '_' + df['order_timestamp'].dt.strftime('%Y%m%d').fillna('Unknown')
    df['time_of_day'] = 'Lunch'
    df.loc[df['order_timestamp'].dt.hour >= 16, 'time_of_day'] = 'Dinner'
    df.loc[df['order_timestamp'].isna(), 'time_of_day'] = 'Unknown'
    df['gross_sales'] = pd.to_numeric(df['Subtotal'], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'
    df['commission_ex_vat'] = pd.to_numeric(df['Deliveroo commission'], errors='coerce').fillna(0).abs()
    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]

def process_takeaway(df):
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip().str.replace('"', '').str.replace("'", "")
    o_col = next((c for c in df.columns if 'order' in c.lower()), 'Order')
    p_col = next((c for c in df.columns if 'pickup' in c.lower()), 'Pickup')
    d_col = next((c for c in df.columns if 'date' in c.lower()), 'Date')
    t_col = next((c for c in df.columns if 'total amount' in c.lower() or 'total' in c.lower()), 'Total amount')

    df['order_timestamp'] = pd.to_datetime(df[d_col], errors='coerce')
    df['order_id'] = 'TA_' + df[o_col].astype(str) + '_' + df['order_timestamp'].dt.strftime('%Y%m%d').fillna('Unknown')
    df['channel'] = np.where(df[p_col].astype(str).str.strip().str.lower() == 'yes', 'Takeaway', 'Delivery') if p_col in df.columns else 'Delivery'
    df['source'], df['time_of_day'] = 'Takeaway', 'Lunch'
    df.loc[df['order_timestamp'].dt.hour >= 16, 'time_of_day'] = 'Dinner'
    df.loc[df['order_timestamp'].isna(), 'time_of_day'] = 'Unknown'
    
    if df[t_col].dtype == object: df[t_col] = df[t_col].str.replace(',', '.')
    df['gross_sales'] = pd.to_numeric(df[t_col], errors='coerce').fillna(0)
    df['net_sales'] = (df['gross_sales'] / 1.06).round(2)
    df['tax'] = df['gross_sales'] - df['net_sales']
    df['vat_rate'] = '6%'
    df['commission_ex_vat'] = (df['gross_sales'] * 0.30).round(2)
    return df[['order_id','source','channel','order_timestamp','time_of_day','vat_rate','net_sales','tax','gross_sales','commission_ex_vat']]

def save_to_db(clean_df, progress_bar=None):
    if clean_df.empty: return 0, 0
    if progress_bar: progress_bar.progress(0.05, text="Checking database...")

    sources = clean_df['source'].unique().tolist()
    params = {f's{i}': s for i, s in enumerate(sources)}
    
    # Fix voor de SQLAlchemy placeholders:
    placeholders = ', '.join([f":{k}" for k in params.keys()])

    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT order_id, vat_rate FROM sales WHERE source IN ({placeholders})"), params)
        exist_df = pd.DataFrame(res.fetchall(), columns=['order_id', 'vat_rate'])

    if not exist_df.empty:
        exist_df['u'] = exist_df['order_id'].str.replace('LS_K-Series_', 'LS_').str.replace('LS_L-Series_', 'LS_')
        exist_keys = set(exist_df['u'].astype(str) + '||' + exist_df['vat_rate'].astype(str).str.strip())
    else:
        exist_keys = set()

    clean_df = clean_df.copy()
    clean_df['u'] = clean_df['order_id'].str.replace('LS_K-Series_', 'LS_').str.replace('LS_L-Series_', 'LS_')
    clean_df['_k'] = clean_df['u'].astype(str) + '||' + clean_df['vat_rate'].astype(str).str.strip()
    
    new_df = clean_df[~clean_df['_k'].isin(exist_keys)].drop(columns=['_k', 'u']).copy()
    skipped = len(clean_df) - len(new_df)

    if progress_bar: progress_bar.progress(0.2, text=f"Found {len(new_df)} new rows...")
    if new_df.empty: return 0, skipped

    new_df = new_df.astype(object).where(pd.notna(new_df), None)
    records, chunk_size, inserted, total = new_df.to_dict(orient="records"), 500, 0, len(new_df)

    for i in range(0, total, chunk_size):
        chunk = records[i : i + chunk_size]
        with engine.begin() as conn:
            res = conn.execute(text("""
                INSERT INTO sales (order_id, source, channel, order_timestamp, time_of_day, vat_rate, net_sales, tax, gross_sales, commission_ex_vat)
                VALUES (:order_id, :source, :channel, :order_timestamp, :time_of_day, :vat_rate, :net_sales, :tax, :gross_sales, :commission_ex_vat)
            """), chunk)
            inserted += res.rowcount
        if progress_bar: progress_bar.progress(0.2 + 0.8 * min((i + chunk_size) / total, 1.0), text=f"Saving: {min(i + chunk_size, total)}/{total}")

    return inserted, skipped

@st.cache_data(ttl=60)
def load_data(full_history=False):
    q = "SELECT * FROM sales ORDER BY order_timestamp" if full_history else "SELECT * FROM sales WHERE order_timestamp >= now() - interval '36 months' ORDER BY order_timestamp"
    df = pd.read_sql(q, engine)
    if not df.empty:
        df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], errors='coerce')
        df = df.dropna(subset=['order_timestamp']).copy()
        if not df.empty:
            df['order_date'] = df['order_timestamp'].dt.date
            df['year'], df['month'] = df['order_timestamp'].dt.year, df['order_timestamp'].dt.to_period('M').astype(str)
            df['week_str'], df['quarter'] = df['order_timestamp'].dt.strftime('%G-W%V'), df['order_timestamp'].dt.to_period('Q').astype(str)
            if 'commission_ex_vat' in df.columns: df['commission_ex_vat'] = pd.to_numeric(df['commission_ex_vat'], errors='coerce').fillna(0.0)
    return df

st.set_page_config(page_title="Restaurant OS", layout="wide")
init_db()

st.sidebar.title("Data Sync")
msg_placeholder = st.sidebar.empty()
if 'import_msg' in st.session_state: msg_placeholder.success(st.session_state.pop('import_msg'))

with st.sidebar.expander("How to update?", expanded=True):
    st.markdown("- **K-Series:** Reports → Receipts → Export CSV\n- **Uber Eats:** Reports → Payment Details → CSV\n- **Deliveroo:** Reports → Orders → CSV\n- **Takeaway:** Invoicing → Orders → CSV")

src_opt = st.sidebar.selectbox("Source", ["Lightspeed K-Series","Lightspeed L-Series","Deliveroo","Uber Eats","Takeaway"])
uploaded_files = st.sidebar.file_uploader("Upload CSV", type=["csv"], accept_multiple_files=True)

if st.sidebar.button("Process File(s)"):
    if uploaded_files:
        all_clean_dfs, t_files = [], len(uploaded_files)
        status_msg = st.sidebar.empty()
        parsers = {"Lightspeed K-Series": process_lightspeed, "Lightspeed L-Series": process_lightspeed, "Uber Eats": process_ubereats, "Deliveroo": process_deliveroo, "Takeaway": process_takeaway}

        for i, file in enumerate(uploaded_files):
            try:
                status_msg.info(f"⏳ File {i+1}/{t_files}: Analyzing...")
                raw = file.read()
                f_line = raw[:1024].decode('utf-8', errors='ignore').split('\n')[0]
                sep = ';' if f_line.count(';') > f_line.count(',') else ','
        
                status_msg.info(f"⚙️ File {i+1}/{t_files}: Parsing data...")
                df_raw = pd.read_csv(io.BytesIO(raw), sep=sep, low_memory=False)
                clean_chunk = parsers[src_opt](df_raw)
                del df_raw
                if not clean_chunk.empty:
                    all_clean_dfs.append(clean_chunk)
            except ValueError as e:
                st.sidebar.error(f"❌ {file.name}: {e}")
            except Exception as e:
                st.sidebar.error(f"Error {file.name}: {e}")

        if not all_clean_dfs:
            st.session_state['import_msg'] = "⚠️ No valid orders found."
        else:
            status_msg.info("🗄️ Finalizing...")
            combined_df = pd.concat(all_clean_dfs, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['order_id', 'vat_rate'])
            
            prog = st.sidebar.progress(0, text="Checking duplicates...")
            ins, skip = save_to_db(combined_df, prog)
            prog.empty(); status_msg.empty()
            st.session_state['import_msg'] = f"✅ Success: {ins} inserted, {skip} skipped."
            
        st.cache_data.clear()
        st.rerun()

data = load_data()
if not data.empty:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📅 Data currently in DB:**")
    sd = data.groupby('source')['order_timestamp'].agg(['min', 'max']).reset_index()
    for _, r in sd.iterrows(): st.sidebar.caption(f"**{r['source']}**\n{r['min'].date()} to {r['max'].date()}")

st.sidebar.subheader("Maintenance")
if st.sidebar.button("🧹 Clean Database"):
    with st.spinner("Cleaning..."):
        try:
            with engine.begin() as conn:
                err_del = conn.execute(text("DELETE FROM sales WHERE source = 'Lightspeed L-Series' AND order_id LIKE 'LS_L-Series_R%'")).rowcount
                c_before = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
                conn.execute(text("""DELETE FROM sales WHERE id IN (SELECT id FROM (SELECT id, ROW_NUMBER() OVER(PARTITION BY REPLACE(REPLACE(order_id, 'LS_K-Series_', 'LS_'), 'LS_L-Series_', 'LS_'), vat_rate ORDER BY CASE WHEN source = 'Lightspeed K-Series' THEN 1 ELSE 2 END, id ASC) as row_num FROM sales) t WHERE t.row_num > 1)"""))
                dup_del = c_before - conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
                conn.execute(text("DELETE FROM sales WHERE order_timestamp IS NULL"))
                tot = err_del + dup_del
                st.session_state['import_msg'] = f"✅ Cleaned! {err_del} invalid, {dup_del} duplicates deleted." if tot > 0 else "DB is clean!"
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Error: {e}")

tab_dash, tab_vat = st.tabs(["Management Dashboard", "VAT Report"])

with tab_dash:
    st.header("Management Dashboard")
    if data.empty:
        st.info("No data in the system yet.")
    else:
        min_db, max_db = data['order_date'].min(), data['order_date'].max()
        dr = st.date_input("Filter Date Range", [min_db, max_db], min_value=min_db, max_value=max_db)
        if len(dr) != 2: st.warning("Select start and end date."); st.stop()
        dd = data[(data['order_date'] >= dr[0]) & (data['order_date'] <= dr[1])]
            
        tot = dd['gross_sales'].sum()
        days = dd['order_date'].nunique() or 1
        lunch = dd[dd['time_of_day']=='Lunch']['gross_sales'].sum()
        dinner = dd[dd['time_of_day']=='Dinner']['gross_sales'].sum()

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Revenue", f"€{tot:,.2f}"); c2.metric("Avg / day", f"€{tot/days:,.2f}")
        c3.metric("Lunch", f"€{lunch:,.2f}", f"{lunch/tot*100:.1f}%" if tot else "")
        c4.metric("Dinner", f"€{dinner:,.2f}", f"{dinner/tot*100:.1f}%" if tot else "")

        col1, col2 = st.columns(2)
        with col1:
            vw = st.radio("View", ["Weekly","Monthly"], horizontal=True)
            gc = 'week_str' if vw == 'Weekly' else 'month'
            tr = dd.groupby([gc,'year'])['gross_sales'].sum().reset_index()
            if not tr.empty: st.plotly_chart(px.bar(tr, x=gc, y='gross_sales', color='year', barmode='group', title="Revenue Trend"), use_container_width=True)
        with col2:
            bc = dd.groupby('channel')['gross_sales'].sum().reset_index()
            if not bc.empty: st.plotly_chart(px.pie(bc, values='gross_sales', names='channel', hole=0.4, title="Revenue by Channel"), use_container_width=True)

        bs = dd.groupby(['source','time_of_day'])['gross_sales'].sum().reset_index()
        if not bs.empty: st.plotly_chart(px.bar(bs, x='source', y='gross_sales', color='time_of_day', title="Revenue by Source & Time of Day"), use_container_width=True)

with tab_vat:
    st.header("VAT Report")
    vat_data = load_data(full_history=True)
    if vat_data.empty: st.info("No data.")
    else:
        qs = sorted([str(q) for q in vat_data['quarter'].unique() if str(q) not in ['NaT', 'nan', 'None']], reverse=True)
        if qs:
            sel_q = st.selectbox("Select Quarter", qs)
            qd = vat_data[vat_data['quarter'] == sel_q]

            cols = ['net_sales','tax','gross_sales', 'commission_ex_vat'] if 'commission_ex_vat' in qd.columns else ['net_sales','tax','gross_sales']
            summ = qd.groupby(['source','vat_rate'])[cols].sum().reset_index()
            summ.columns = ['Source', 'VAT Rate', 'Net', 'VAT', 'Gross', 'Commission (ex VAT)'] if len(cols)==4 else ['Source', 'VAT Rate', 'Net', 'VAT', 'Gross']

            td = {'Source': 'TOTAL', 'VAT Rate': '', 'Net': summ['Net'].sum(), 'VAT': summ['VAT'].sum(), 'Gross': summ['Gross'].sum()}
            if 'Commission (ex VAT)' in summ.columns: td['Commission (ex VAT)'] = summ['Commission (ex VAT)'].sum()
            
            st.subheader("Overview")
            fmt = {'Net': '€{:.2f}', 'VAT': '€{:.2f}', 'Gross': '€{:.2f}'}
            if 'Commission (ex VAT)' in summ.columns: fmt['Commission (ex VAT)'] = '€{:.2f}'
                
            st.dataframe(pd.concat([summ, pd.DataFrame([td])], ignore_index=True).style.format(fmt), use_container_width=True)
            
            with st.expander("Show Details"):
                c_show = ['order_id', 'source', 'channel', 'order_timestamp', 'time_of_day', 'vat_rate', 'net_sales', 'tax', 'gross_sales']
                if 'commission_ex_vat' in qd.columns: c_show.append('commission_ex_vat')
                st.dataframe(qd[c_show], use_container_width=True)

            if st.button("Export to Excel"):
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='openpyxl') as w:
                    summ.to_excel(w, sheet_name='Summary', index=False)
                    qd[c_show].to_excel(w, sheet_name='Details', index=False)
                st.download_button("📥 Download Excel", out.getvalue(), file_name=f"VAT_Report_{sel_q}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
