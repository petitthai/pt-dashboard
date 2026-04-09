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
            CREATE INDEX IF NOT EXISTS idx_sales_source_order_vat
                ON sales(source, order_id, vat_rate)
        '''))

def get_time_of_day(hour):
    if pd.isna(hour): return 'Unknown'
    return 'Lunch' if hour < 16 else 'Dinner'

def process_lightspeed(df, version="K-Series"):
    df.columns = df.columns.astype(str).str.strip()

    # ── Detect format: K-series heeft 'Identifier', L-series niet ──
    is_kseries = 'Identifier' in df.columns

    # ✅ AUTO-CORRECTIE: Forceer de juiste versie op basis van het bestand, ongeacht de dropdown keuze!
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

    df['order_id']        = f'LS_{version}_' + df[receipt_col].astype(str)
    df['channel']         = df['Type'].apply(lambda x: 'Takeaway' if str(x).lower()=='takeaway' else 'In-Restaurant') if 'Type' in df.columns else 'In-Restaurant'
    df['order_timestamp'] = pd.to_datetime(df[date_col], dayfirst=True)
    df['time_of_day']     = df['order_timestamp'].dt.hour.apply(get_time_of_day)
    df['source']          = f'Lightspeed {version}'

    has_tax_col = 'Taxes' in df.columns
    has_vat     = has_tax_col and df['Taxes'].astype(str).str.contains('=').any()

    if has_vat:
        base = df[['order_id','source','channel','order_timestamp','time_of_day']].copy()
        base['vat_parts'] = df['Taxes'].astype(str).str.split('|')
        exploded = base.explode('vat_parts')
        exploded = exploded[exploded['vat_parts'].str.contains('=', na=False)].copy()

        split = exploded['vat_parts'].str.strip().str.split('=', expand=True)
        exploded['rate']     = pd.to_numeric(split[0].str.replace('%','').str.strip(), errors='coerce')
        exploded['tax']      = pd.to_numeric(split[1].str.strip(), errors='coerce')
        exploded = exploded.dropna(subset=['rate','tax'])
        exploded = exploded[exploded['rate'] > 0]

        exploded['net_sales']   = (exploded['tax'] / (exploded['rate'] / 100)).round(2)
        exploded['gross_sales'] = (exploded['net_sales'] + exploded['tax']).round(2)
        exploded['vat_rate']    = exploded['rate'].astype(int).astype(str) + '%'

        return exploded[['order_id','source','channel','order_timestamp','time_of_day',
                          'vat_rate','net_sales','tax','gross_sales']].reset_index(drop=True)
    else:
        gross_col = next((c for c in ['Total','Total incl. Tax'] if c in df.columns), None)
        net_col   = next((c for c in ['Net Total','Total excl. Tax'] if c in df.columns), None)
        df['gross_sales'] = pd.to_numeric(df[gross_col], errors='coerce').fillna(0) if gross_col else 0.0
        df['net_sales']   = pd.to_numeric(df[net_col],   errors='coerce').fillna(0) if net_col else 0.0
        df['tax']         = df['gross_sales'] - df['net_sales']
        df['vat_rate']    = 'Mixed'
        return df[['order_id','source','channel','order_timestamp','time_of_day',
                   'vat_rate','net_sales','tax','gross_sales']].reset_index(drop=True)
    
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
    df.columns = df.columns.astype(str).str.strip().str.replace('"', '').str.replace("'", "")
    
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

def save_to_db_with_progress(clean_df, progress_bar=None):
    if clean_df.empty:
        return 0, 0

    sources = clean_df['source'].unique().tolist()
    placeholders = ', '.join(f':s{i}' for i in range(len(sources)))

    if progress_bar:
        progress_bar.progress(0.05, text="Controleren op bestaande data in de database...")

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT order_id, vat_rate FROM sales WHERE source IN ({placeholders})"),
            {f's{i}': s for i, s in enumerate(sources)}
        )
        existing_df = pd.DataFrame(result.fetchall(), columns=['order_id', 'vat_rate'])

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
        progress_bar.progress(0.2, text=f"Gevonden: {len(new_df)} nieuwe rijen ({skipped} dubbels overgeslagen)...")

    if new_df.empty:
        return 0, skipped

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
                text=f"Database bijwerken... {min(i + chunk_size, total)}/{total} rijen toegevoegd"
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

with st.sidebar.expander("Hoe updaten?", expanded=True):
    st.markdown("""
- **K-Series:** Backoffice → Reports → Receipts → Export CSV  
- **Uber Eats:** UE Manager → Payments → Invoices → Export CSV  
- **Deliveroo:** Hub → Invoices → Orders → Export CSV  
- **Takeaway:** Portal → Invoicing → Orders → Export CSV
    """)

source_option = st.sidebar.selectbox("Bronbestand", ["Lightspeed K-Series","Lightspeed L-Series","Deliveroo","Uber Eats","Takeaway"])
uploaded_file = st.sidebar.file_uploader("Upload CSV", type=["csv"])

if st.sidebar.button("Verwerk Bestand"):
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

            with st.spinner("Bestand analyseren..."):
                clean_df = parsers[source_option](df_raw)

            if clean_df.empty:
                st.session_state['import_msg'] = "⚠️ Geen nieuwe of voltooide bestellingen gevonden in dit bestand."
            else:
                progress = st.sidebar.progress(0, text="Zoeken naar dubbele orders...")
                inserted, skipped = save_to_db_with_progress(clean_df, progress)
                progress.empty()

                st.session_state['import_msg'] = (
                    f"✅ Import geslaagd: {inserted} nieuwe rijen toegevoegd, {skipped} dubbele overgeslagen."
                )
                
            st.cache_data.clear()
            st.rerun()
            
        except Exception as e:
            st.sidebar.error(f"Fout bij inlezen van bestand: {e}")

data = load_data(full_history=False)

if not data.empty:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📅 Data momenteel in DB:**")
    summary_dates = data.groupby('source')['order_timestamp'].agg(['min', 'max']).reset_index()
    for _, row in summary_dates.iterrows():
        src = row['source']
        min_date = row['min'].date()
        max_date = row['max'].date()
        st.sidebar.caption(f"**{src}**\n{min_date} tot {max_date}")
    st.sidebar.markdown("---")

st.sidebar.subheader("Onderhoud")
if st.sidebar.button("🧹 Clean Database"):
    with st.spinner("Database wordt grondig schoongemaakt..."):
        try:
            with engine.begin() as conn:
                # ✅ STAP 1: Verwijder de "K-Series in schaapskleren"
                # Als het order_id in L-Series zit, maar het begint met de letter R, moet het weg!
                result_fout = conn.execute(text("""
                    DELETE FROM sales 
                    WHERE source = 'Lightspeed L-Series' 
                    AND order_id LIKE 'LS_L-Series_R%'
                """))
                foute_verwijderd = result_fout.rowcount

                # ✅ STAP 2: Standaard check voor exacte dubbele rijen
                count_before = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
                conn.execute(text("""
                    DELETE FROM sales
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER(
                                PARTITION BY order_id, vat_rate 
                                ORDER BY id ASC
                            ) as row_num
                            FROM sales
                        ) t WHERE t.row_num > 1
                    )
                """))
                count_after = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
                dubbels_verwijderd = count_before - count_after
                
                totaal_verwijderd = foute_verwijderd + dubbels_verwijderd
                
                if totaal_verwijderd > 0:
                    st.session_state['import_msg'] = f"✅ Opgeschoond! {foute_verwijderd} foute L-Series verwijderd en {dubbels_verwijderd} dubbele rijen gewist."
                else:
                    st.session_state['import_msg'] = "Database is al helemaal schoon! Geen foute formaten of dubbels gevonden."
                    
            st.cache_data.clear()
            st.rerun()
            
        except Exception as e:
            st.sidebar.error(f"Fout bij schoonmaken: {e}")

tab_dash, tab_vat = st.tabs(["Management Dashboard", "Btw Rapportage (VAT)"])

with tab_dash:
    st.header("Management Dashboard")
    if data.empty:
        st.info("Nog geen data in het systeem — upload een CSV via het menu links.")
    else:
        min_date_db = data['order_date'].min()
        max_date_db = data['order_date'].max()
        
        date_range = st.date_input("Filter Dashboard Periode", [min_date_db, max_date_db], min_value=min_date_db, max_value=max_date_db)
        
        if len(date_range) != 2:
            st.warning("Selecteer aub een start- en einddatum om het dashboard te zien.")
            st.stop()
            
        dash_data = data[(data['order_date'] >= date_range[0]) & (data['order_date'] <= date_range[1])]
            
        total = dash_data['gross_sales'].sum()
        days = dash_data['order_date'].nunique() or 1
        lunch = dash_data[dash_data['time_of_day']=='Lunch']['gross_sales'].sum()
        dinner = dash_data[dash_data['time_of_day']=='Dinner']['gross_sales'].sum()

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Totale Omzet", f"€{total:,.2f}")
        c2.metric("Gem. per geopende dag", f"€{total/days:,.2f}")
        c3.metric("Lunch", f"€{lunch:,.2f}", f"{lunch/total*100:.1f}%" if total else "")
        c4.metric("Diner", f"€{dinner:,.2f}", f"{dinner/total*100:.1f}%" if total else "")

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            view = st.radio("Weergave", ["Wekelijks","Maandelijks"], horizontal=True)
            grp_col = 'week_str' if view == 'Wekelijks' else 'month'
            trend = dash_data.groupby([grp_col,'year'])['gross_sales'].sum().reset_index()
            if not trend.empty:
                fig = px.bar(trend, x=grp_col, y='gross_sales', color='year', barmode='group', title="Omzet Trend")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            by_channel = dash_data.groupby('channel')['gross_sales'].sum().reset_index()
            if not by_channel.empty:
                fig2 = px.pie(by_channel, values='gross_sales', names='channel', hole=0.4, title="Verdeling per Kanaal")
                st.plotly_chart(fig2, use_container_width=True)

        by_src = dash_data.groupby(['source','time_of_day'])['gross_sales'].sum().reset_index()
        if not by_src.empty:
            fig3 = px.bar(by_src, x='source', y='gross_sales', color='time_of_day', title="Verdeling Bron vs Moment (Lunch/Diner)")
            st.plotly_chart(fig3, use_container_width=True)

with tab_vat:
    st.header("Btw Rapportage (VAT)")
    
    vat_data = load_data(full_history=True)
    
    if vat_data.empty:
        st.info("Nog geen data in de rapportage.")
    else:
        quarters = sorted(vat_data['quarter'].unique(), reverse=True)
        selected_q = st.selectbox("Selecteer Kwartaal", quarters)
        q_data = vat_data[vat_data['quarter'] == selected_q]

        summary = q_data.groupby(['source','vat_rate'])[['net_sales','tax','gross_sales']].sum().reset_index()
        summary.columns = ['Bron', 'Btw-tarief', 'Netto', 'Btw-bedrag', 'Bruto']

        totals = pd.DataFrame([{
            'Bron': 'TOTAAL', 'Btw-tarief': '',
            'Netto': summary['Netto'].sum(),
            'Btw-bedrag': summary['Btw-bedrag'].sum(),
            'Bruto': summary['Bruto'].sum()
        }])
        
        st.subheader("Overzicht per bron")
        st.dataframe(
            pd.concat([summary, totals], ignore_index=True)
              .style.format({'Netto': '€{:.2f}', 'Btw-bedrag': '€{:.2f}', 'Bruto': '€{:.2f}'}),
            use_container_width=True
        )
        
        with st.expander("Toon alle individuele transacties (inclusief Order ID)"):
            st.dataframe(
                q_data[['order_id', 'source', 'channel', 'order_timestamp', 'time_of_day', 'vat_rate', 'net_sales', 'tax', 'gross_sales']],
                use_container_width=True
            )

        if st.button("Exporteer naar Excel"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                summary.to_excel(writer, sheet_name='1. Samenvatting', index=False)
                q_data[['order_id','source','channel','order_timestamp',
                         'time_of_day','vat_rate','net_sales','tax','gross_sales']]\
                    .to_excel(writer, sheet_name='2. Alle_Transacties_Details', index=False)
                    
            st.download_button("📥 Download Excel Bestand", output.getvalue(),
                file_name=f"Btw_Rapportage_{selected_q}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
