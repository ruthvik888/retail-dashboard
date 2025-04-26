import os
from flask import Flask, render_template, request, flash, redirect, url_for
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET', 'change-me')

# Azure Blob config from environment
blob_conn = os.environ['AZURE_BLOB_CONN_STR']
container = os.environ['AZURE_CONTAINER']
blob_svc  = BlobServiceClient.from_connection_string(blob_conn)

def load_csv(name):
    b = blob_svc.get_blob_client(container=container, blob=name)
    data = b.download_blob().readall()
    df = pd.read_csv(BytesIO(data))
    # normalize column names
    df.columns = df.columns.str.strip().str.upper()
    return df

# Load once on start
df_house = load_csv('400_households.csv')
df_tx    = load_csv('400_transactions.csv')
df_prod  = load_csv('400_products.csv')

# ensure numeric keys
df_house['HSHD_NUM']    = pd.to_numeric(df_house['HSHD_NUM'],    errors='coerce')
df_tx   ['HSHD_NUM']    = pd.to_numeric(df_tx   ['HSHD_NUM'],    errors='coerce')
df_tx   ['PRODUCT_NUM'] = pd.to_numeric(df_tx   ['PRODUCT_NUM'], errors='coerce')
df_prod ['PRODUCT_NUM'] = pd.to_numeric(df_prod ['PRODUCT_NUM'], errors='coerce')

@app.route('/')
def login():
    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

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
