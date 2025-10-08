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
    Simulate quota usage after adding the uploaded deliveries, using:
      - max quota from FARMERS (authoritative)
      - current delivered total from QUOTA_VIEW (if present)
    """

    # (Optional but recommended) refresh the MV so totals aren't stale
    try:
        supabase.rpc("refresh_quota_view").execute()
        # tiny pause helps if DB is busy
        time.sleep(0.3)
    except Exception:
        pass  # non-fatal in comparison mode

    # 1) Load current totals from quota_view
    qv = supabase.table("quota_view").select("farmer_id,total_net_weight_kg").execute()
    qv_df = pd.DataFrame(qv.data) if qv.data else pd.DataFrame(columns=["farmer_id","total_net_weight_kg"])
    qv_df.columns = qv_df.columns.str.lower()
    if not qv_df.empty:
        qv_df["farmer_id"] = qv_df["farmer_id"].astype(str).str.strip().str.lower()
        qv_df["total_net_weight_kg"] = pd.to_numeric(qv_df["total_net_weight_kg"], errors="coerce").fillna(0)
    else:
        qv_df = pd.DataFrame(columns=["farmer_id","total_net_weight_kg"])

    # 2) Build authoritative max_quota_kg per farmer from FARMERS
    # If your farmers_df already has these columns, reuse it; otherwise fetch a slim set:
    # farmers_df = pd.DataFrame(supabase.table("farmers").select("farmer_id,max_quota_kg,hectares,area_ha").execute().data)
    f = farmers_df.copy()
    f.columns = f.columns.str.lower()
    for c in ["farmer_id", "max_quota_kg", "hectares", "area_ha"]:
        if c not in f.columns:
            f[c] = None

    # Normalize farmer_id
    f["farmer_id"] = f["farmer_id"].astype(str).str.strip().str.lower()

    # Clean textified quotas like "5,000" or "5 000"
    def _clean_num(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        if s == "":
            return None
        s = s.replace(" ", "").replace(",", "")
        try:
            return float(s)
        except Exception:
            return None

    f["max_quota_kg_clean"] = f["max_quota_kg"].apply(_clean_num)

    # Pick area column
    area_col = "hectares" if "hectares" in f.columns and f["hectares"].notna().any() else "area_ha"
    f["area_val"] = pd.to_numeric(f[area_col], errors="coerce")

    # Fill missing per-farmer max quota from area * QUOTA_PER_HA (kg/ha)
    f["authoritative_max_quota_kg"] = f["max_quota_kg_clean"]
    f.loc[f["authoritative_max_quota_kg"].isna(), "authoritative_max_quota_kg"] = (
        f.loc[f["authoritative_max_quota_kg"].isna(), "area_val"].fillna(0) * QUOTA_PER_HA
    )

    # 3) Merge current totals with authoritative max
    base = f[["farmer_id", "authoritative_max_quota_kg"]].merge(
        qv_df[["farmer_id", "total_net_weight_kg"]],
        on="farmer_id",
        how="left"
    )
    base["total_net_weight_kg"] = pd.to_numeric(base["total_net_weight_kg"], errors="coerce").fillna(0)
    base["authoritative_max_quota_kg"] = pd.to_numeric(base["authoritative_max_quota_kg"], errors="coerce").fillna(0)

    # 4) Add the uploaded weights
    add = uploaded_df.groupby("farmer_id", as_index=False)["net_weight_kg"].sum()
    add["farmer_id"] = add["farmer_id"].astype(str).str.strip().str.lower()
    add["net_weight_kg"] = pd.to_numeric(add["net_weight_kg"], errors="coerce").fillna(0)

    sim = base.merge(add, on="farmer_id", how="outer", suffixes=("", "_add"))
    sim["total_net_weight_kg"] = sim["total_net_weight_kg"].fillna(0)
    sim["net_weight_kg"] = sim["net_weight_kg"].fillna(0)
    sim["authoritative_max_quota_kg"] = sim["authoritative_max_quota_kg"].fillna(0)

    sim["new_total_kg"] = sim["total_net_weight_kg"] + sim["net_weight_kg"]

    # 5) Compute % + status using authoritative max
    def pct(row):
        denom = row["authoritative_max_quota_kg"]
        return 0.0 if denom <= 0 else (row["new_total_kg"] / denom) * 100.0

    sim["quota_used_pct"] = sim.apply(pct, axis=1)

    def status(row):
        p = row["quota_used_pct"]
        if p > 100:
            return "EXCEEDED"
        elif p > 90:
            return "WARNING"
        return "OK"

    sim["quota_status"] = sim.apply(status, axis=1)

    # 6) Return in your expected columns
    sim.rename(columns={
        "authoritative_max_quota_kg": "max_quota_kg",
        "new_total_kg": "total_net_weight_kg"
    }, inplace=True)

    # Keep only farmers present in the uploaded file (your UI expectation)
    uploaded_ids = uploaded_df["farmer_id"].astype(str).str.strip().str.lower().unique().tolist()
    sim = sim[sim["farmer_id"].isin(uploaded_ids)].copy()

    # Order / formatting
    sim = sim[["farmer_id", "max_quota_kg", "total_net_weight_kg", "quota_used_pct", "quota_status"]]
    return sim


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