import os
from flask import Flask, render_template, request, flash, redirect, url_for, session
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
import matplotlib.pyplot as plt
import base64


app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET')

# Azure Blob config from environment
blob_conn = os.environ['AZURE_BLOB_CONN_STR']
container = os.environ['AZURE_CONTAINER']
blob_svc  = BlobServiceClient.from_connection_string(blob_conn)

def load_csv(blob_name):
    client = blob_svc.get_blob_client(container=container, blob=blob_name)
    data = client.download_blob().readall()
    df = pd.read_csv(BytesIO(data))
    # strip whitespace, uppercase
    df.columns = df.columns.str.strip().str.upper()
    return df

# ─── load your three tables ───────────────────────────────────────────────────
df_house = load_csv('400_households.csv')
df_tx    = load_csv('400_transactions.csv')
df_prod  = load_csv('400_products.csv')

# ─── align column names with what the CSV actually uses ────────────────────────
# Households table already has HSHD_NUM
# Transactions table uses PURCHASE_DATE (or PURCHASE_) as the date column
if 'PURCHASE_' in df_tx.columns:
    df_tx.rename(columns={'PURCHASE_': 'DATE'}, inplace=True)
elif 'PURCHASE_DATE' in df_tx.columns:
    df_tx.rename(columns={'PURCHASE_DATE': 'DATE'}, inplace=True)
else:
    raise RuntimeError("Couldn't find the transaction date column!")

# Products table has BRAND_TY but you may want BRAND_TYPE later:
if 'BRAND_TY' in df_prod.columns:
    df_prod.rename(columns={'BRAND_TY': 'BRAND_TYPE'}, inplace=True)

# ─── coerce your join keys to numeric ─────────────────────────────────────────
df_house['HSHD_NUM']    = pd.to_numeric(df_house['HSHD_NUM'],    errors='coerce')
df_tx   ['HSHD_NUM']    = pd.to_numeric(df_tx   ['HSHD_NUM'],    errors='coerce')
df_tx   ['PRODUCT_NUM'] = pd.to_numeric(df_tx   ['PRODUCT_NUM'], errors='coerce')
df_prod ['PRODUCT_NUM'] = pd.to_numeric(df_prod ['PRODUCT_NUM'], errors='coerce')

def plot_to_base64(fig):
    buf = BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

@app.route('/')
def login():
    return render_template('login.html')
    

@app.route('/dashboard')
def dashboard():
    if 'user' not in session:
        flash("Please log in first", "warning")
        return redirect(url_for('login'))

    merged = df_tx.merge(df_house, on='HSHD_NUM') \
                  .merge(df_prod, on='PRODUCT_NUM')
    merged['DATE'] = pd.to_datetime(merged['DATE'], errors='coerce')

    # ─── Chart 1: Total Spend Over Time ───
    monthly = merged.groupby(merged['DATE'].dt.to_period('M'))['SPEND'].sum().reset_index()
    fig1, ax1 = plt.subplots()
    ax1.plot(monthly['DATE'].astype(str), monthly['SPEND'], marker='o')
    ax1.set_title("Total Spend Over Time")
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Total Spend ($)")
    ax1.tick_params(axis='x', rotation=45)
    fig1.tight_layout()
    spend_plot = plot_to_base64(fig1)

    # ─── Chart 2: Brand Preference (Pie) ───
    brand_counts = merged['BRAND_TYPE'].value_counts()
    fig2, ax2 = plt.subplots()
    ax2.pie(brand_counts, labels=brand_counts.index, autopct='%1.1f%%', startangle=90)
    ax2.set_title("Brand Preference")
    brand_plot = plot_to_base64(fig2)

    # ─── Chart 3: Organic vs Non-Organic ───
    if 'ORGANIC' in merged.columns:
        organic_counts = merged['ORGANIC'].value_counts()
        fig3, ax3 = plt.subplots()
        ax3.bar(organic_counts.index.astype(str), organic_counts.values)
        ax3.set_title("Organic vs Non-Organic")
        ax3.set_xlabel("Organic")
        ax3.set_ylabel("Count")
        fig3.tight_layout()
        organic_plot = plot_to_base64(fig3)
    else:
        organic_plot = None

    # ─── Chart 4: Spend by Category Over Time ───
    top_commodities = merged['COMMODITY'].value_counts().head(5).index
    filtered = merged[merged['COMMODITY'].isin(top_commodities)]
    filtered['MONTH'] = filtered['DATE'].dt.to_period('M')
    pivot = filtered.groupby(['MONTH', 'COMMODITY'])['SPEND'].sum().unstack().fillna(0)
    fig4, ax4 = plt.subplots()
    pivot.plot(ax=ax4, marker='o')
    ax4.set_title("Monthly Spend by Top 5 Commodities")
    ax4.set_xlabel("Month")
    ax4.set_ylabel("Spend ($)")
    ax4.tick_params(axis='x', rotation=45)
    fig4.tight_layout()
    category_plot = plot_to_base64(fig4)

    # ─── Chart 5: Cross-Selling Pairs ───
    basket_products = merged.groupby(['HSHD_NUM', 'BASKET_NUM'])['COMMODITY'].apply(list)
    from collections import Counter
    pair_counter = Counter()
    for items in basket_products:
        unique = list(set(items))
        for i in range(len(unique)):
            for j in range(i + 1, len(unique)):
                pair = tuple(sorted((unique[i], unique[j])))
                pair_counter[pair] += 1
    top_pairs = pair_counter.most_common(5)
    pair_labels = [f"{a} + {b}" for (a, b), _ in top_pairs]
    pair_values = [v for _, v in top_pairs]
    fig5, ax5 = plt.subplots()
    ax5.barh(pair_labels[::-1], pair_values[::-1])
    ax5.set_title("Top 5 Product Pairs (Cross-Selling)")
    ax5.set_xlabel("Frequency")
    fig5.tight_layout()
    cross_plot = plot_to_base64(fig5)

    return render_template('dashboard.html',
                           spend_plot=spend_plot,
                           brand_plot=brand_plot,
                           organic_plot=organic_plot,
                           category_plot=category_plot,
                           cross_plot=cross_plot)


@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        try:
            h = int(request.form['hshd_num'])
        except ValueError:
            flash("Enter a valid Household #", "danger")
            return redirect(url_for('search'))
        hh = df_house[df_house['HSHD_NUM']==h]
        merged = hh.merge(df_tx, on='HSHD_NUM') \
                   .merge(df_prod, on='PRODUCT_NUM') \
                   .sort_values(['HSHD_NUM','BASKET_NUM','DATE','PRODUCT_NUM','DEPARTMENT','COMMODITY'])
        rows = merged.to_dict('records')
        if not rows:
            flash(f"No data for Household #{h}", "warning")
        return render_template('search_results.html', hshd=h, rows=rows)
    return render_template('search.html')

@app.route('/sample-data')
def sample_data():
    return redirect(url_for('search', **{'hshd_num':10}))

if __name__=='__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
