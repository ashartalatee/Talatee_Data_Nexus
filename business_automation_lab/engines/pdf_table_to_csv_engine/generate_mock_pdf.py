import os
from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

def generate_sales_pdf(output_path: Path, num_rows: int = 150):
    """Membuat file PDF formal berisi tabel transaksi ratusan baris untuk pengujian."""
    
    # Setup dokumen PDF
    doc = SimpleDocTemplate(
        str(output_path), 
        pagesize=letter,
        rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30
    )
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Heading1'],
        fontSize=16,
        leading=20,
        textColor=colors.HexColor('#1A365D'),
        spaceAfter=15
    )
    
    elements = []
    
    # Judul Dokumen
    elements.append(Paragraph("LAPORAN REKAPITULASI TRANSAKSI OMNICHANNEL - Q2 2026", title_style))
    elements.append(Paragraph("Dokumen internal hasil ekspor sistem data nexus agensi.", styles['Normal']))
    elements.append(Spacer(1, 15))
    
    # Header Tabel
    table_data = [[
        "No", "Transaction ID", "Date", "Channel", "SKU Product", "Qty", "Revenue (IDR)", "Status"
    ]]
    
    # Generate baris data dummy (Ratusan Baris)
    channels = ["Shopee", "Tokopedia", "TikTok Shop", "Lazada"]
    skus = ["SKU-DRY-A1", "SKU-ROUTER-X", "SKU-SMART-P2", "SKU-MICE-WL"]
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "CANCELED"]
    
    for i in range(1, num_rows + 1):
        channel = channels[i % len(channels)]
        sku = skus[i % len(skus)]
        status = statuses[i % len(statuses)]
        revenue = f"{(150000 + (i * 2500)):,}".replace(",", ".")
        
        row = [
            str(i),
            f"TX-20260611-{i:03d}",
            "11/06/2026",
            channel,
            sku,
            str((i % 3) + 1),
            revenue,
            status
        ]
        table_data.append(row)
    
    # Desain Tabel Standar Korporat agar Bagus Ditampilkan di Video
    t = Table(table_data, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1A365D')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F7FAFC')),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#E2E8F0')),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EDF2F7')]), # Efek warna belang-belang (zebra)
    ]))
    
    elements.append(t)
    doc.build(elements)
    print(f"✅ Sukses generate file PDF dummy sebanyak {num_rows} baris di: {output_path}")

if __name__ == "__main__":
    # Tentukan target folder input
    ROOT_DIR = Path(__file__).resolve().parent
    INPUT_DIR = ROOT_DIR / "input"
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    target_file = INPUT_DIR / "REKAP_TRANSAKSI_MARKETPLACE_LARGE.pdf"
    
    # Kita buat 150 baris data (secara visual otomatis akan memecah menjadi sekitar 4-5 halaman PDF)
    generate_sales_pdf(target_file, num_rows=150)