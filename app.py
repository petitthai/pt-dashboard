def process_ubereats(df):
    if 'Order ID as per Uber Eats manager' in df.columns:
        new_header = df.iloc[0]
        df = df[1:].copy()
        df.columns = new_header
        
    df.columns = df.columns.str.strip()
    df = df[df['Order status'] == 'Completed'].copy()
    
    rows = []
    
    for _, r in df.iterrows():
        order_id = 'UE_' + str(r['Order ID'])
        timestamp = pd.to_datetime(str(r['Order date']) + ' ' + str(r['Order confirmed time']), dayfirst=True, errors='coerce')
        time_of_day = get_time_of_day(timestamp.hour if pd.notnull(timestamp) else None)
        
        # Uber separates VAT into VAT 1, VAT 2, VAT 3
        vats = {
            'VAT 1': pd.to_numeric(r.get('VAT 1 on sales', 0), errors='coerce'),
            'VAT 2': pd.to_numeric(r.get('VAT 2 on sales', 0), errors='coerce'),
            'VAT 3': pd.to_numeric(r.get('VAT 3 on sales', 0), errors='coerce')
        }
        
        total_net = pd.to_numeric(r.get('Sales (excl. VAT)', 0), errors='coerce')
        total_gross = pd.to_numeric(r.get('Sales (incl. VAT)', 0), errors='coerce')
        
        # If there's absolutely no VAT recorded, log it as 0%
        if sum(vats.values()) == 0:
            rows.append({
                'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                'order_timestamp': timestamp, 'time_of_day': time_of_day,
                'vat_rate': '0%', 'net_sales': total_net, 'tax': 0.0, 'gross_sales': total_gross
            })
            continue

        # Reverse engineer the net sales for each VAT bracket
        for vat_name, tax_amount in vats.items():
            if pd.isna(tax_amount) or tax_amount <= 0:
                continue
                
            # We deduce the rate by rounding. E.g. in Belgium, common rates are 6%, 12%, 21%
            # If tax is 4.64 and we know it's VAT1 (usually 6%), we can find the net: net = tax / 0.06
            # We'll calculate the implied rate if we compare it to common Belgian rates:
            rate = "Mixed"
            net_for_this_tax = 0
            
            # Simple heuristic for Belgium VAT rates based on the tax amount
            # Uber Eats usually maps VAT 1 = 6%, VAT 2 = 21% (or similar).
            # To be mathematically safe, if tax_amount > 0, we can estimate the rate:
            if round(tax_amount / 0.06, 2) <= total_net + 1 and vat_name == 'VAT 1':
                rate = "6%"
                net_for_this_tax = round(tax_amount / 0.06, 2)
            elif round(tax_amount / 0.21, 2) <= total_net + 1 and vat_name == 'VAT 2':
                rate = "21%"
                net_for_this_tax = round(tax_amount / 0.21, 2)
            else:
                # Fallback if it's an unusual percentage: just label it by the Uber column
                rate = f"Uber {vat_name}"
                net_for_this_tax = round(tax_amount / 0.06, 2) # rough fallback

            gross_for_this_tax = round(net_for_this_tax + tax_amount, 2)
            
            rows.append({
                'order_id': order_id, 'source': 'Uber Eats', 'channel': 'Delivery',
                'order_timestamp': timestamp, 'time_of_day': time_of_day,
                'vat_rate': rate, 'net_sales': net_for_this_tax, 'tax': tax_amount, 'gross_sales': gross_for_this_tax
            })
            
    return pd.DataFrame(rows)
