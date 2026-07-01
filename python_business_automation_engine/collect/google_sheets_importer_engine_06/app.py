"""
Google Sheets Importer Engine — Antarmuka untuk Pengguna Non-IT
==================================================================
Cara menjalankan:
    streamlit run app.py

Tidak perlu menyentuh kode sama sekali. Cukup:
1. Tempel link Google Sheets
2. Klik "Import Data"
3. Lihat preview, lalu unduh sebagai CSV / Excel / JSON
"""

import io
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent / "src"))
from sheets_importer.importer import GoogleSheetsImporter, GoogleSheetsImporterError  # noqa: E402

st.set_page_config(page_title="Google Sheets Importer Engine", page_icon="📥", layout="wide")

st.title("📥 Google Sheets Importer Engine")
st.caption("Collect Engine #6 — Python Business Automation Engine")
st.write(
    "Tempel link Google Sheets di bawah, klik **Import Data**, lalu unduh hasilnya. "
    "Tidak perlu coding sama sekali."
)

with st.sidebar:
    st.header("⚙️ Mode Akses")
    mode = st.radio(
        "Pilih mode",
        ["🌐 Sheet Publik (mudah, tanpa setup)", "🔒 Sheet Privat (perlu Service Account)"],
        index=0,
    )
    creds_file = None
    if mode.startswith("🔒"):
        st.info(
            "Upload file credentials.json Service Account Anda. "
            "Panduan lengkap ada di docs/SETUP_GOOGLE_API.md"
        )
        creds_file = st.file_uploader("Upload credentials.json", type=["json"])
    st.divider()
    st.caption("Bagian dari portofolio Python Business Automation Engine")

sheet_url = st.text_input(
    "🔗 Link Google Sheets",
    placeholder="https://docs.google.com/spreadsheets/d/xxxxxxxx/edit#gid=0",
)

worksheet_name = None
if mode.startswith("🔒"):
    worksheet_name = st.text_input("Nama Worksheet/Tab (kosongkan untuk tab pertama)")

run = st.button("🚀 Import Data", type="primary")

if "df" not in st.session_state:
    st.session_state.df = None
    st.session_state.summary = None

if run:
    if not sheet_url.strip():
        st.error("Mohon masukkan link Google Sheets terlebih dahulu.")
    else:
        with st.spinner("Mengambil data dari Google Sheets..."):
            try:
                credentials_path = None
                if creds_file is not None:
                    credentials_path = f"/tmp/{creds_file.name}"
                    with open(credentials_path, "wb") as f:
                        f.write(creds_file.getbuffer())

                importer = GoogleSheetsImporter(credentials_path=credentials_path)

                if mode.startswith("🌐"):
                    df = importer.from_public_link(sheet_url)
                else:
                    df = importer.from_private_sheet(sheet_url, worksheet_name or None)

                st.session_state.df = df
                st.session_state.summary = importer.get_summary()
                st.success(f"Berhasil! {len(df)} baris × {len(df.columns)} kolom berhasil diimpor.")
            except GoogleSheetsImporterError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"Terjadi kesalahan tak terduga: {e}")

if st.session_state.df is not None:
    df = st.session_state.df

    st.subheader("👀 Preview Data")
    st.dataframe(df.head(50), use_container_width=True)

    st.subheader("⬇️ Unduh Hasil")
    c1, c2, c3 = st.columns(3)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    with c1:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Unduh CSV", csv_bytes, f"data_{timestamp}.csv", "text/csv", use_container_width=True
        )
    with c2:
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine="openpyxl")
        st.download_button(
            "Unduh Excel",
            excel_buffer.getvalue(),
            f"data_{timestamp}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with c3:
        json_bytes = df.to_json(orient="records", indent=2, force_ascii=False).encode("utf-8")
        st.download_button(
            "Unduh JSON", json_bytes, f"data_{timestamp}.json", "application/json", use_container_width=True
        )

    with st.expander("ℹ️ Detail Import"):
        st.json(st.session_state.summary)
