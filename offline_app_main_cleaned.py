import streamlit as st
import pandas as pd
from datetime import datetime
from fpdf import FPDF
from io import BytesIO
from PIL import Image
from supabase import create_client, Client
import re
import time
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

st.set_page_config(page_title="CloudIA Quota Verifier", layout="centered")
QUOTA_PER_HA = 800
LOGO_PATH = "cloudia_logo.png"
LOGO_COCOA = "cocoasourcelogo.jpg"

# --- Language Switcher ---
lang = st.sidebar.radio("🌐 Language / Langue", ["English", "Français"])

def t(key):
    translations = {
        "upload_title": {
            "English": "Upload Delivery Template",
            "Français": "Téléverser le Modèle de Livraison"
        },
        "file_format_caption": {
            "English": "✅ Format: .xlsx | Max size: 200MB",
            "Français": "✅ Format : .xlsx | Taille max : 200 Mo"
        },
        "title": {
            "English": "☁️ CloudIA – Farmer Quota Verification System – Coop Level",
            "Français": "☁️ CloudIA – Système de Vérification des Quotas – Niveau Coopérative"
        },
        "missing_exporter_column": {
            "English": "Missing 'exporter' column in the Excel file.",
            "Français": "La colonne 'exporter' est manquante dans le fichier Excel."
        },
        "missing_columns": {
            "English": "Missing columns: {}",
            "Français": "Colonnes manquantes : {}"
        },
        "unknown_farmers_error": {
            "English": "The following farmers are NOT in the database:",
            "Français": "Les producteurs suivants ne sont PAS dans la base de données :"
        },
        "quota_overview_title": {
            "English": "### Quota Overview (Only Warnings and Exceeded)",
            "Français": "### Aperçu des Quotas (Avertissements et Dépassements)"
        },
        "quota_warning_count": {
            "English": "⚠️ {} farmers in the uploaded file have quota warnings or exceeded limits.",
            "Français": "⚠️ {} producteurs ont des avertissements ou des dépassements de quota."
        },
        "quota_ok": {
            "English": "✅ All farmers in the uploaded file are within their assigned quotas.",
            "Français": "✅ Tous les producteurs respectent leurs quotas."
        },
        "lot_status_out_of_range": {
            "English": "### Lot Status Overview - Out of Range",
            "Français": "### Aperçu des Lots - Hors Plage Autorisée"
        },
        "file_approved": {
            "English": "✅ File approved. All farmers valid, quotas OK, and delivered kg per lot within allowed range.",
            "Français": "✅ Fichier approuvé. Tous les producteurs sont valides et les quotas respectés."
        },
        "generate_pdf": {
            "English": "Generate Approval PDF",
            "Français": "Générer le PDF d'Approbation"
        },
        "download_pdf": {
            "English": "Download Approval PDF",
            "Français": "Télécharger le PDF"
        },
        "lot_too_low": {
            "English": "Too low",
            "Français": "Trop faible"
        },
        "lot_within_range": {
            "English": "Within range",
            "Français": "Dans la plage autorisée"
        },
        "validation_complete": {
            "English": "✅ Validation completed successfully!",
            "Français": "✅ Validation terminée avec succès !"
        },
        "comparison_mode": {
            "English": "📊 Comparison Mode - File validation only (no data saved)",
            "Français": "📊 Mode Comparaison - Validation uniquement (aucune donnée sauvegardée)"
        }
    }
    return translations.get(key, {}).get(lang, key)

sharepoint_config = st.secrets.get("sharepoint", {})

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

def simulate_quota_check(uploaded_df, farmers_df):
    """
    Simulate quota checking by calculating what the quotas would be 
    if this delivery were added to existing deliveries
    """
    # Get current quota status from database
    result = supabase.table("quota_view").select("*").execute()
    current_quota_df = pd.DataFrame(result.data)
    
    if current_quota_df.empty:
        # If no existing quota data, create basic structure
        current_quota_df = pd.DataFrame({
            'farmer_id': farmers_df['farmer_id'],
            'max_quota_kg': QUOTA_PER_HA * 1000,  # Assuming 1 hectare per farmer as default
            'total_net_weight_kg': 0,
            'quota_used_pct': 0,
            'quota_status': 'OK'
        })
    
    # Normalize columns
    current_quota_df.columns = current_quota_df.columns.str.strip().str.lower()
    current_quota_df['farmer_id'] = current_quota_df['farmer_id'].astype(str).str.strip().str.lower()
    
    # Calculate additional weight from uploaded file
    uploaded_weights = uploaded_df.groupby('farmer_id')['net_weight_kg'].sum().reset_index()
    uploaded_weights['farmer_id'] = uploaded_weights['farmer_id'].astype(str).str.strip().str.lower()
    
    # Merge and calculate new totals
    simulated_quota = current_quota_df.copy()
    
    for _, row in uploaded_weights.iterrows():
        farmer_id = row['farmer_id']
        additional_weight = row['net_weight_kg']
        
        # Find farmer in current quota data
        farmer_idx = simulated_quota[simulated_quota['farmer_id'] == farmer_id].index
        
        if len(farmer_idx) > 0:
            idx = farmer_idx[0]
            current_weight = simulated_quota.loc[idx, 'total_net_weight_kg']
            max_quota = simulated_quota.loc[idx, 'max_quota_kg']
            
            new_total = current_weight + additional_weight
            new_percentage = (new_total / max_quota) * 100 if max_quota > 0 else 0
            
            # Update simulated values
            simulated_quota.loc[idx, 'total_net_weight_kg'] = new_total
            simulated_quota.loc[idx, 'quota_used_pct'] = new_percentage
            
            # Determine status
            if new_percentage > 100:
                simulated_quota.loc[idx, 'quota_status'] = 'EXCEEDED'
            elif new_percentage > 90:
                simulated_quota.loc[idx, 'quota_status'] = 'WARNING'
            else:
                simulated_quota.loc[idx, 'quota_status'] = 'OK'
        else:
            # Farmer not found in quota data - add them
            # You might want to fetch max_quota from farmers table based on hectares
            farmer_data = farmers_df[farmers_df['farmer_id'] == farmer_id]
            if not farmer_data.empty:
                # Assuming there's a hectares column, otherwise default to 1
                hectares = farmer_data.iloc[0].get('hectares', 1)
                max_quota = hectares * QUOTA_PER_HA
                
                new_percentage = (additional_weight / max_quota) * 100 if max_quota > 0 else 0
                
                if new_percentage > 100:
                    status = 'EXCEEDED'
                elif new_percentage > 90:
                    status = 'WARNING'
                else:
                    status = 'OK'
                
                new_row = pd.DataFrame({
                    'farmer_id': [farmer_id],
                    'max_quota_kg': [max_quota],
                    'total_net_weight_kg': [additional_weight],
                    'quota_used_pct': [new_percentage],
                    'quota_status': [status]
                })
                simulated_quota = pd.concat([simulated_quota, new_row], ignore_index=True)
    
    return simulated_quota

def generate_pdf_confirmation(lot_numbers, exporter_name, farmer_count, total_kg, lot_kg_summary, logo_path, logo_cocoa):
    from fpdf import FPDF
    from io import BytesIO

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, "Delivery Validation Report", ln=True, align="C")

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
    pdf.multi_cell(0, 10, f"Status: VALIDATION ONLY - NO DATA SAVED")

    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Lot Summary", ln=True)
    pdf.set_font("Arial", "", 12)
    for lot, kg in lot_kg_summary.items():
        pdf.cell(0, 10, f"{lot}: {round(kg / 1000, 2)} MT", ln=True)

    pdf.ln(5)
    pdf.cell(0, 10, "Validated by CloudIA (Comparison Mode)", ln=True)

    # Generate filename
    reference_number = str(lot_numbers[0]).replace("/", "_") if len(lot_numbers) == 1 else "MULTI"
    today_str = datetime.now().strftime('%Y%m%d')
    exporter_clean = exporter_name.replace(" ", "_").replace("/", "_")[:20]
    total_volume_mt = round(total_kg / 1000, 2)
    filename = f"Validation_{reference_number}_{today_str}_{exporter_clean}_{total_volume_mt}MT.pdf"

    pdf_bytes = pdf.output(dest='S').encode('latin1')
    pdf_buffer = BytesIO(pdf_bytes)
    pdf_buffer.seek(0)

    return filename, pdf_buffer

def upload_to_sharepoint(file_buffer, filename, sharepoint_config):
    try:
        site_url = sharepoint_config["site_url"]
        client_id = sharepoint_config["client_id"]
        client_secret = sharepoint_config["client_secret"]
        library_name = sharepoint_config["library_name"]

        credentials = ClientCredential(client_id, client_secret)
        ctx = ClientContext(site_url).with_credentials(credentials)

        folder_url = f"/sites/TRACAFILES/{library_name}"
        target_folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        ctx.load(target_folder)
        ctx.execute_query()

        target_folder.upload_file(filename, file_buffer.getvalue()).execute_query()
        st.success(f"✅ File uploaded to SharePoint: {filename}")
        return True

    except Exception as e:
        st.error(f"❌ Upload failed. Error:\n\n{e}")
        return False

# --- UI Layout ---
st.markdown("---")
logo_col1, logo_col2 = st.columns([1, 1])
with logo_col1:
    st.image(Image.open(LOGO_PATH), width=120)
with logo_col2:
    st.image(Image.open(LOGO_COCOA), width=200)

st.markdown("""
    <div style='
        text-align: center;
        font-size: 60px;
        font-weight: bold;
        margin-top: 30px;
        margin-bottom: 20px;
        color: #2c3e50;
        letter-spacing: 5px;
    '>
        COOP
    </div>
""", unsafe_allow_html=True)

st.markdown(f"### {t('title')}")

# Add comparison mode indicator
st.info(t("comparison_mode"))

# --- Main Logic ---
st.subheader("📥 Step 1: Upload Excel for Validation")
delivery_file = st.file_uploader(t("upload_title"), type=["xlsx"])
farmers_df = load_all_farmers()

if delivery_file:
    uploaded_df = pd.read_excel(delivery_file)
    uploaded_df.columns = uploaded_df.columns.str.strip().str.lower()
    uploaded_df['farmer_id'] = uploaded_df['farmer_id'].astype(str).str.strip().str.lower()

    if 'exporter' not in uploaded_df.columns:
        st.error(t("missing_exporter_column"))
        st.stop()

    exporter_names = uploaded_df['exporter'].dropna().astype(str).str.strip().unique()
    exporter_name = ", ".join(exporter_names)

    expected_columns = ['cooperative name', 'export lot n°/connaissement', 'date of purchase from cooperative',
                        'certification', 'farmer_id', 'farm_id', 'net weight (kg)', 'exporter']
    missing_columns = [col for col in expected_columns if col not in uploaded_df.columns]
    if missing_columns:
        st.error(t("missing_columns").format(', '.join(missing_columns)))
        st.stop()

    uploaded_df.rename(columns={
        'export lot n°/connaissement': 'export_lot',
        'net weight (kg)': 'net_weight_kg',
        'date of purchase from cooperative': 'purchase_date'
    }, inplace=True)

    uploaded_df['purchase_date'] = uploaded_df['purchase_date'].fillna(datetime.today().strftime('%Y-%m-%d'))
    uploaded_df['exporter'] = exporter_name

    uploaded_df = uploaded_df.drop_duplicates(subset=['export_lot', 'exporter', 'farmer_id', 'net_weight_kg'], keep='last')

    # Check if farmers exist in database
    unknown_farmers = uploaded_df[
        ~uploaded_df['farmer_id'].str.lower().isin(farmers_df['farmer_id'].str.lower())
    ]['farmer_id'].unique()

    if unknown_farmers.size > 0:
        st.error(t("unknown_farmers_error"))
        st.write(list(unknown_farmers))
        st.stop()

    # Simulate quota checking without saving data
    simulated_quota_df = simulate_quota_check(uploaded_df, farmers_df)
    
    # Filter for uploaded farmers only
    uploaded_ids = pd.Series(uploaded_df['farmer_id']).astype(str).str.strip().str.lower()
    quota_filtered = simulated_quota_df[
        (simulated_quota_df['farmer_id'].isin(uploaded_ids)) & 
        (simulated_quota_df['quota_status'].isin(['EXCEEDED', 'WARNING']))
    ]

    if not quota_filtered.empty:
        st.write(t("quota_overview_title"))

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
        st.warning(t("quota_warning_count").format(len(quota_filtered)))
    else:
        st.success(t("quota_ok"))

    # Check lot weights
    lot_totals = uploaded_df.groupby('export_lot')['net_weight_kg'].sum()

    def check_lot_status(weight_in_kg):
        weight_in_mt = weight_in_kg / 1000
        if weight_in_mt < 21:
            return t("lot_too_low")
        else:
            return t("lot_within_range")

    lot_status = lot_totals.apply(check_lot_status)
    lot_status_ok = lot_status == t("lot_within_range")

    lot_status_info = pd.DataFrame({
        'export_lot': lot_totals.index,
        'total_net_weight_kg': lot_totals.values,
        'lot_status': lot_status
    })

    if not lot_status_ok.all():
        st.write(t("lot_status_out_of_range"))
        st.dataframe(lot_status_info[~lot_status_ok])

    # Final validation results
    all_ids_valid = len(unknown_farmers) == 0
    any_quota_exceeded = 'EXCEEDED' in quota_filtered['quota_status'].values if not quota_filtered.empty else False

    if all_ids_valid and not any_quota_exceeded and lot_status_ok.all():
        st.success(t("file_approved"))
    else:
        st.success(t("validation_complete"))

    # PDF Generation
    if 'pdf_buffer' not in st.session_state:
        st.session_state['pdf_buffer'] = None
        st.session_state['pdf_filename'] = None

    col1, col2 = st.columns([1, 1])

    with col1:
        if st.button(t("generate_pdf")):
            total_kg = int(lot_totals.sum())
            filename, pdf_buffer = generate_pdf_confirmation(
                lot_numbers=lot_totals.index.tolist(),
                exporter_name=exporter_name,
                farmer_count=uploaded_df['farmer_id'].nunique(),
                total_kg=total_kg,
                lot_kg_summary=lot_totals.to_dict(),
                logo_path=LOGO_PATH,
                logo_cocoa=LOGO_COCOA
            )
            st.session_state['pdf_buffer'] = pdf_buffer
            st.session_state['pdf_filename'] = filename

            st.download_button(
                label=t("download_pdf"),
                data=pdf_buffer,
                file_name=filename,
                mime="application/pdf"
            )

    with col2:
        if st.button("📤 Upload to SharePoint"):
            success_pdf = success_excel = False

            if st.session_state['pdf_buffer'] and st.session_state['pdf_filename']:
                st.info("📤 Uploading PDF to SharePoint...")
                success_pdf = upload_to_sharepoint(
                    st.session_state['pdf_buffer'],
                    st.session_state['pdf_filename'],
                    sharepoint_config
                )

                st.info("📤 Uploading Excel to SharePoint...")
                delivery_file.seek(0)
                success_excel = upload_to_sharepoint(
                    delivery_file,
                    delivery_file.name,
                    sharepoint_config
                )

                if success_pdf and success_excel:
                    st.success("✅ Both PDF and Excel uploaded to SharePoint.")
                else:
                    st.warning("⚠️ Not all files uploaded. See error above.")
            else:
                st.warning("⚠️ Please generate the PDF first.")