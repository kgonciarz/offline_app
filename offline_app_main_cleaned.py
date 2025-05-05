import streamlit as st
import pandas as pd
from datetime import datetime
from fpdf import FPDF
from io import BytesIO
from PIL import Image
from supabase import create_client, Client
import re
import time

QUOTA_PER_HA = 800
LOGO_PATH = "cloudia_logo.png"
LOGO_COCOA = "cocoasourcelogo.jpg"

@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

supabase = get_supabase()

@st.cache_data
def load_all_farmers():
    all_rows = []
    page_size = 1000
    last_farmer_id = None
    while True:
        query = supabase.table("farmers").select("*").limit(page_size).order("farmer_id")
        if last_farmer_id:
            query = query.gt("farmer_id", last_farmer_id)
        result = query.execute()
        rows = result.data
        if not rows:
            break
        all_rows.extend(rows)
        last_farmer_id = rows[-1]["farmer_id"]
    farmers_df = pd.DataFrame(all_rows)
    farmers_df.columns = farmers_df.columns.str.lower()
    farmers_df['farmer_id'] = farmers_df['farmer_id'].astype(str).str.strip().str.lower()
    return farmers_df

def delete_existing_delivery_rpc(export_lot, exporter_name, farmer_ids):
    export_lot = str(export_lot)
    exporter_name = str(exporter_name)
    if hasattr(farmer_ids, 'tolist'):
        farmer_ids = farmer_ids.tolist()
    farmer_ids = [str(farmer_id) for farmer_id in farmer_ids]
    try:
        supabase.rpc('delete_traceability_records', {
            'lot': export_lot,
            'exporter_param': exporter_name,
            'farmer_ids': farmer_ids
        }).execute()
    except Exception as e:
        st.error(f"❌ RPC Delete Error: {e}")


def save_delivery_to_supabase(df):
    column_mapping = {
        'cooperative name': 'cooperative_name',
        'export lot n°/connaissement': 'export_lot',
        'date of purchase from cooperative': 'purchase_date',
        'certification': 'certification',
        'farmer_id': 'farmer_id',
        'net weight (kg)': 'net_weight_kg',
        'exporter': 'exporter'
    }
    df = df.rename(columns=column_mapping)
    required_columns = ['export_lot', 'exporter', 'farmer_id', 'net_weight_kg']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        st.error(f"Missing required columns: {', '.join(missing_columns)}")
        return False

    df_cleaned = df.copy()
    df_cleaned['farmer_id'] = df_cleaned['farmer_id'].str.strip().str.lower()
    df_cleaned['purchase_date'] = df_cleaned['purchase_date'].fillna(datetime.today().strftime('%Y-%m-%d'))

    def excel_date_to_date(excel_date):
        if isinstance(excel_date, (int, float)):
            return (pd.to_datetime('1899-12-30') + pd.to_timedelta(excel_date, unit='D')).strftime('%Y-%m-%d')
        return excel_date

    df_cleaned['purchase_date'] = df_cleaned['purchase_date'].apply(excel_date_to_date)
    df_cleaned['purchase_date'] = df_cleaned['purchase_date'].astype(str)
    data = df_cleaned.to_dict(orient="records")

    try:
        supabase.table("traceability").insert(data).execute()
        st.success(f"✅ Data successfully inserted! {len(data)} new records added.")
        return True
    except Exception as e:
        st.error(f"❌ Error while inserting into traceability table: {e}")
        return False
def refresh_quota_view():
    try:
        supabase.rpc("refresh_quota_view").execute()
        print("✅ quota_view successfully refreshed.")
    except Exception as e:
        print("❌ Failed to refresh quota_view:", e)

refresh_quota_view()

def generate_pdf_confirmation(lot_numbers, exporter_name, farmer_count, total_kg, lot_kg_summary, logo_path, logo_cocoa):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, "Delivery Approval Certificate", ln=True, align="C")

    if logo_path:
        pdf.image(logo_path, x=10, y=20, w=40)
    if logo_cocoa:
        pdf.image(logo_cocoa, x=(210 - 110) / 2, y=20, w=110)

    pdf.set_y(70)
    pdf.set_font("Arial", "", 12)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    pdf.multi_cell(0, 10, f"Generated on: {now}")
    pdf.multi_cell(0, 10, f"Exporter: {exporter_name}")
    pdf.multi_cell(0, 10, f"Lots: {', '.join(str(l) for l in lot_numbers)}")
    pdf.multi_cell(0, 10, f"Total Farmers: {farmer_count}")
    pdf.multi_cell(0, 10, f"Total Net Weight: {round(total_kg / 1000, 2)} MT")

    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Lot Summary", ln=True)
    pdf.set_font("Arial", "", 12)
    for lot, kg in lot_kg_summary.items():
        pdf.cell(0, 10, f"{lot}: {round(kg / 1000, 2)} MT", ln=True)

    pdf.ln(5)
    pdf.cell(0, 10, "Approved by CloudIA", ln=True)

    reference_number = lot_numbers[0] if len(lot_numbers) == 1 else "MULTI"
    today_str = datetime.now().strftime('%Y%m%d')
    exporter_clean = exporter_name.replace(" ", "_").replace("/", "_")[:20]
    total_volume_mt = round(total_kg / 1000, 2)

    filename = f"Approval_{reference_number}_{today_str}_{exporter_clean}_{total_volume_mt}MT.pdf"
    pdf.output(filename)

    # --- ZAPISZ DO TABELI approvals ---
    data = {
        "created_at": now,
        "lot_number": ", ".join(str(l) for l in lot_numbers),
        "exporter_name": exporter_name,
        "approved_by": "CloudIA",
        "file_name": filename
    }
    try:
        supabase.table("approvals").insert(data).execute()
    except Exception as e:
        st.error(f"❌ Error saving approval to DB: {e}")

    return filename


def load_quota_view():
    result = supabase.table("quota_view").select("*").execute()
    return pd.DataFrame(result.data)

# --- UI Layout ---
col1, col2 = st.columns(2)
with col1:
    st.image(Image.open(LOGO_PATH), width=150)
with col2:
    st.image(Image.open(LOGO_COCOA), width=300)

st.title("CloudIA - Farmer Quota Verification System")

# --- Główna logika ---
delivery_file = st.sidebar.file_uploader("Upload Delivery Template", type=["xlsx"])
farmers_df = load_all_farmers()

if delivery_file:
    uploaded_df = pd.read_excel(delivery_file)
    uploaded_df.columns = uploaded_df.columns.str.strip().str.lower()
    uploaded_df['farmer_id'] = uploaded_df['farmer_id'].astype(str).str.strip().str.lower()

    if 'exporter' not in uploaded_df.columns:
        st.error("Missing 'exporter' column in the Excel file.")
        st.stop()

    exporter_names = uploaded_df['exporter'].dropna().astype(str).str.strip().unique()
    exporter_name = ", ".join(exporter_names)

    expected_columns = ['cooperative name', 'export lot n°/connaissement', 'date of purchase from cooperative',
                        'certification', 'farmer_id', 'farm_id', 'net weight (kg)', 'exporter']
    missing_columns = [col for col in expected_columns if col not in uploaded_df.columns]
    if missing_columns:
        st.error(f"Missing columns: {', '.join(missing_columns)}")
        st.stop()

    uploaded_df.rename(columns={
        'export lot n°/connaissement': 'export_lot',
        'net weight (kg)': 'net_weight_kg',
        'date of purchase from cooperative': 'purchase_date'
    }, inplace=True)

    uploaded_df['purchase_date'] = uploaded_df['purchase_date'].fillna(datetime.today().strftime('%Y-%m-%d'))
    uploaded_df['exporter'] = exporter_name

    uploaded_df = uploaded_df.drop_duplicates(subset=['export_lot', 'exporter', 'farmer_id', 'net_weight_kg'], keep='last')

    unknown_farmers = uploaded_df[
        ~uploaded_df['farmer_id'].str.lower().isin(farmers_df['farmer_id'].str.lower())
    ]['farmer_id'].unique()

    if unknown_farmers.size > 0:
        st.error("The following farmers are NOT in the database:")
        st.write(list(unknown_farmers))
        st.stop()

    lot_numbers = uploaded_df['export_lot'].unique()
    for lot in lot_numbers:
        farmer_ids_for_lot = uploaded_df[uploaded_df['export_lot'] == lot]['farmer_id'].unique().tolist()
        delete_existing_delivery_rpc(lot, exporter_name, farmer_ids_for_lot)

# ... (wszystko przed tym zostaje bez zmian)

    inserted_ok = save_delivery_to_supabase(uploaded_df)
    if not inserted_ok:
        st.stop()

    time.sleep(1)  # daj czas na propagację danych
    quota_df = load_quota_view()

    # Diagnoza – sprawdź czy kolumna farmer_id istnieje
    if 'farmer_id' not in quota_df.columns:
        st.error("❌ quota_view does not contain 'farmer_id'. Columns returned: " + str(list(quota_df.columns)))
        st.stop()

    # Czyszczenie i filtrowanie
    uploaded_ids = pd.Series(uploaded_df['farmer_id']).astype(str).str.strip().str.lower()
    quota_df['farmer_id'] = quota_df['farmer_id'].astype(str).str.strip().str.lower()
    quota_df = quota_df[quota_df['farmer_id'].isin(uploaded_ids)]

    quota_filtered = quota_df[quota_df['quota_status'].isin(['EXCEEDED', 'WARNING'])]


    if not quota_filtered.empty:
        st.write("### Quota Overview (Only Warnings and Exceeded)")

        def highlight_status(val):
            if val == 'EXCEEDED':
                return 'background-color: #ffcccc'
            elif val == 'WARNING':
                return 'background-color: #fff3cd'
            return ''

        styled_quota = quota_filtered[[
            'farmer_id', 'max_quota_kg', 'total_net_weight_kg', 'quota_used_pct', 'quota_status'
        ]].style.applymap(highlight_status, subset=['quota_status']).format({
            'max_quota_kg': '{:.0f}',
            'total_net_weight_kg': '{:.0f}',
            'quota_used_pct': '{:.2f}'
        })

        st.dataframe(styled_quota, use_container_width=True)
        st.warning(f"⚠️ {len(quota_filtered)} farmers in the uploaded file have quota warnings or exceeded limits.")
    else:
        st.success("✅ All farmers in the uploaded file are within their assigned quotas.")

    all_ids_valid = len(unknown_farmers) == 0
    any_quota_exceeded = 'EXCEEDED' in quota_filtered['quota_status'].values
    lot_totals = uploaded_df.groupby('export_lot')['net_weight_kg'].sum()

    def check_lot_status(weight_in_kg):
        weight_in_mt = weight_in_kg / 1000
        if weight_in_mt < 21:
            return "Too low"
        elif weight_in_mt > 29:
            return "Too high"
        else:
            return "Within range"

    lot_status = lot_totals.apply(check_lot_status)
    lot_status_ok = lot_status == "Within range"

    lot_status_info = pd.DataFrame({
        'export_lot': lot_totals.index,
        'total_net_weight_kg': lot_totals.values,
        'lot_status': lot_status
    })

    if not lot_status_ok.all():
        st.write("### Lot Status Overview - Out of Range")
        st.dataframe(lot_status_info[~lot_status_ok])

    def rollback_delivery(uploaded_df):
        lot_numbers = uploaded_df['export_lot'].unique()
        exporter_name = uploaded_df['exporter'].iloc[0]
        for lot in lot_numbers:
            farmer_ids_for_lot = uploaded_df[uploaded_df['export_lot'] == lot]['farmer_id'].unique().tolist()
            delete_existing_delivery_rpc(lot, exporter_name, farmer_ids_for_lot)
        st.error("❌ Uploaded delivery has been rolled back due to validation errors. PDF cannot be generated.")

    if all_ids_valid and not any_quota_exceeded and lot_status_ok.all():
        st.success("✅ File approved. All farmers valid, quotas OK, and delivered kg per lot within allowed range.")
        if st.button("Generate Approval PDF"):
            total_kg = int(lot_totals.sum())
            pdf_file = generate_pdf_confirmation(
                lot_numbers=lot_totals.index.tolist(),
                exporter_name=exporter_name,
                farmer_count=uploaded_df['farmer_id'].nunique(),
                total_kg=total_kg,
                lot_kg_summary=lot_totals.to_dict(),
                logo_path=LOGO_PATH,
                logo_cocoa=LOGO_COCOA
            )
            with open(pdf_file, "rb") as f:
                st.download_button("Download Approval PDF", data=f, file_name=pdf_file, mime="application/pdf")
    else:
        rollback_delivery(uploaded_df)
