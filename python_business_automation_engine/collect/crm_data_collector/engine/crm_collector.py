"""
╔══════════════════════════════════════════════════════════════════╗
║          CRM DATA COLLECTOR ENGINE v1.0                          ║
║          Python Business Automation Engine #08                   ║
║          By: [Your Name] | Portfolio Project                     ║
╚══════════════════════════════════════════════════════════════════╝

DESKRIPSI:
    Engine ini mengumpulkan data pelanggan dari berbagai sumber
    (CSV, Excel, Google Sheets, Form Input Manual) dan menyimpannya
    ke dalam satu database CRM terpusat secara otomatis.

COCOK UNTUK:
    - UMKM yang belum punya sistem CRM
    - Tim sales yang masih pakai spreadsheet manual
    - Freelancer yang ingin kelola klien lebih profesional

CARA PAKAI (Non-IT):
    1. Double-click file ini ATAU jalankan: python crm_collector.py
    2. Pilih sumber data (CSV / Excel / Input Manual)
    3. Klik tombol "Proses Data"
    4. File CRM otomatis tersimpan di folder /output
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import sqlite3
import os
import json
import re
from datetime import datetime
from pathlib import Path


# ─────────────────────────────────────────────
#  KONFIGURASI ENGINE
# ─────────────────────────────────────────────
CONFIG = {
    "app_name": "CRM Data Collector Engine",
    "version": "1.0.0",
    "output_dir": Path(__file__).parent.parent / "output",
    "db_name": "crm_database.db",
    "export_formats": ["Excel (.xlsx)", "CSV (.csv)", "JSON (.json)"],
}

# Pastikan folder output ada
CONFIG["output_dir"].mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
#  LAYER 1: DATABASE (Penyimpanan Data CRM)
# ─────────────────────────────────────────────
class CRMDatabase:
    """Mengelola penyimpanan data CRM ke SQLite"""

    def __init__(self):
        db_path = CONFIG["output_dir"] / CONFIG["db_name"]
        self.conn = sqlite3.connect(str(db_path))
        self._create_tables()

    def _create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nama        TEXT NOT NULL,
                email       TEXT,
                telepon     TEXT,
                perusahaan  TEXT,
                kategori    TEXT DEFAULT 'Prospect',
                sumber      TEXT,
                catatan     TEXT,
                tanggal     TEXT,
                status      TEXT DEFAULT 'Active'
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name   TEXT,
                total_rows  INTEGER,
                success     INTEGER,
                failed      INTEGER,
                tanggal     TEXT
            )
        """)
        self.conn.commit()

    def insert_customer(self, data: dict) -> bool:
        """Simpan satu data customer ke database"""
        try:
            self.conn.execute("""
                INSERT INTO customers 
                (nama, email, telepon, perusahaan, kategori, sumber, catatan, tanggal, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get("nama", ""),
                data.get("email", ""),
                data.get("telepon", ""),
                data.get("perusahaan", ""),
                data.get("kategori", "Prospect"),
                data.get("sumber", "Manual"),
                data.get("catatan", ""),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                data.get("status", "Active"),
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"[DB ERROR] {e}")
            return False

    def insert_many(self, records: list, sumber: str = "Import") -> dict:
        """Simpan banyak customer sekaligus (bulk insert)"""
        success = 0
        failed = 0
        for rec in records:
            rec["sumber"] = sumber
            if self.insert_customer(rec):
                success += 1
            else:
                failed += 1

        # Catat ke log
        self.conn.execute("""
            INSERT INTO import_log (file_name, total_rows, success, failed, tanggal)
            VALUES (?, ?, ?, ?, ?)
        """, (sumber, len(records), success, failed, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.conn.commit()
        return {"success": success, "failed": failed}

    def get_all(self) -> pd.DataFrame:
        """Ambil semua data customer"""
        return pd.read_sql("SELECT * FROM customers ORDER BY id DESC", self.conn)

    def search(self, keyword: str) -> pd.DataFrame:
        """Cari customer berdasarkan nama/email/perusahaan"""
        q = f"%{keyword}%"
        return pd.read_sql("""
            SELECT * FROM customers 
            WHERE nama LIKE ? OR email LIKE ? OR perusahaan LIKE ?
            ORDER BY id DESC
        """, self.conn, params=(q, q, q))

    def get_stats(self) -> dict:
        """Statistik ringkas untuk dashboard"""
        df = self.get_all()
        if df.empty:
            return {"total": 0, "prospect": 0, "customer": 0, "inactive": 0}
        return {
            "total": len(df),
            "prospect": len(df[df["kategori"] == "Prospect"]),
            "customer": len(df[df["kategori"] == "Customer"]),
            "inactive": len(df[df["status"] == "Inactive"]),
        }

    def export(self, fmt: str) -> str:
        """Export semua data ke file"""
        df = self.get_all()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if "xlsx" in fmt:
            path = CONFIG["output_dir"] / f"crm_export_{ts}.xlsx"
            df.to_excel(path, index=False)
        elif "csv" in fmt:
            path = CONFIG["output_dir"] / f"crm_export_{ts}.csv"
            df.to_csv(path, index=False)
        else:
            path = CONFIG["output_dir"] / f"crm_export_{ts}.json"
            df.to_json(path, orient="records", indent=2, force_ascii=False)
        return str(path)


# ─────────────────────────────────────────────
#  LAYER 2: DATA COLLECTOR (Pengumpul Data)
# ─────────────────────────────────────────────
class DataCollector:
    """Mengumpulkan dan membersihkan data dari berbagai sumber"""

    COLUMN_MAP = {
        # Mapping nama kolom umum → nama standar CRM
        "name": "nama", "full name": "nama", "customer name": "nama",
        "nama pelanggan": "nama", "nama lengkap": "nama",
        "e-mail": "email", "mail": "email", "email address": "email",
        "phone": "telepon", "no hp": "telepon", "handphone": "telepon",
        "mobile": "telepon", "no telepon": "telepon", "whatsapp": "telepon",
        "company": "perusahaan", "company name": "perusahaan",
        "nama perusahaan": "perusahaan", "instansi": "perusahaan",
        "type": "kategori", "tipe": "kategori", "customer type": "kategori",
        "note": "catatan", "notes": "catatan", "keterangan": "catatan",
        "remark": "catatan",
    }

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standarisasi nama kolom dari berbagai format"""
        rename = {}
        for col in df.columns:
            normalized = col.strip().lower()
            if normalized in self.COLUMN_MAP:
                rename[col] = self.COLUMN_MAP[normalized]
        return df.rename(columns=rename)

    def _clean_phone(self, phone) -> str:
        """Bersihkan nomor telepon ke format standar"""
        if pd.isna(phone) or not str(phone).strip():
            return ""
        phone = re.sub(r"[^\d+]", "", str(phone))
        if phone.startswith("08"):
            phone = "+628" + phone[2:]
        elif phone.startswith("8") and len(phone) >= 9:
            phone = "+628" + phone[1:]
        return phone

    def _clean_email(self, email) -> str:
        """Validasi format email"""
        if pd.isna(email):
            return ""
        email = str(email).strip().lower()
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return email if re.match(pattern, email) else ""

    def from_csv(self, filepath: str) -> list:
        """Baca dan proses data dari file CSV"""
        df = pd.read_csv(filepath, encoding="utf-8-sig")
        return self._process_df(df, source="CSV Import")

    def from_excel(self, filepath: str) -> list:
        """Baca dan proses data dari file Excel"""
        df = pd.read_excel(filepath)
        return self._process_df(df, source="Excel Import")

    def _process_df(self, df: pd.DataFrame, source: str) -> list:
        """Proses DataFrame: normalize, clean, return list of dict"""
        df = self._normalize_columns(df)
        records = []
        for _, row in df.iterrows():
            record = {
                "nama": str(row.get("nama", "")).strip(),
                "email": self._clean_email(row.get("email", "")),
                "telepon": self._clean_phone(row.get("telepon", "")),
                "perusahaan": str(row.get("perusahaan", "")).strip(),
                "kategori": str(row.get("kategori", "Prospect")).strip(),
                "catatan": str(row.get("catatan", "")).strip(),
                "sumber": source,
            }
            if record["nama"]:  # minimal harus ada nama
                records.append(record)
        return records


# ─────────────────────────────────────────────
#  LAYER 3: GUI (Antarmuka Pengguna Non-IT)
# ─────────────────────────────────────────────
class CRMApp(tk.Tk):
    """Antarmuka grafis CRM Data Collector untuk pengguna Non-IT"""

    COLORS = {
        "bg": "#0F172A",
        "surface": "#1E293B",
        "card": "#263347",
        "accent": "#3B82F6",
        "accent2": "#10B981",
        "danger": "#EF4444",
        "text": "#F1F5F9",
        "muted": "#94A3B8",
        "border": "#334155",
    }

    def __init__(self):
        super().__init__()
        self.db = CRMDatabase()
        self.collector = DataCollector()
        self._setup_window()
        self._build_ui()
        self._refresh_table()

    def _setup_window(self):
        self.title(f"🚀 {CONFIG['app_name']} v{CONFIG['version']}")
        self.geometry("1100x720")
        self.configure(bg=self.COLORS["bg"])
        self.resizable(True, True)
        # Center window
        self.update_idletasks()
        x = (self.winfo_screenwidth() - 1100) // 2
        y = (self.winfo_screenheight() - 720) // 2
        self.geometry(f"1100x720+{x}+{y}")

    def _build_ui(self):
        """Bangun semua komponen UI"""
        self._build_header()
        self._build_stats_bar()
        self._build_main_content()
        self._build_status_bar()

    def _build_header(self):
        hdr = tk.Frame(self, bg=self.COLORS["accent"], padx=20, pady=12)
        hdr.pack(fill="x")

        tk.Label(hdr, text="🚀 CRM DATA COLLECTOR ENGINE",
                 bg=self.COLORS["accent"], fg="white",
                 font=("Arial", 16, "bold")).pack(side="left")

        tk.Label(hdr, text=f"Engine #08 | Python Automation Portfolio",
                 bg=self.COLORS["accent"], fg="#DBEAFE",
                 font=("Arial", 10)).pack(side="right", padx=10)

    def _build_stats_bar(self):
        self.stats_frame = tk.Frame(self, bg=self.COLORS["surface"], pady=10)
        self.stats_frame.pack(fill="x", padx=10, pady=(5, 0))
        self._update_stats()

    def _update_stats(self):
        for w in self.stats_frame.winfo_children():
            w.destroy()
        stats = self.db.get_stats()
        items = [
            ("👥 Total Data", stats["total"], self.COLORS["accent"]),
            ("🎯 Prospect", stats["prospect"], "#8B5CF6"),
            ("✅ Customer", stats["customer"], self.COLORS["accent2"]),
            ("💤 Inactive", stats["inactive"], self.COLORS["muted"]),
        ]
        for label, value, color in items:
            card = tk.Frame(self.stats_frame, bg=self.COLORS["card"],
                            relief="flat", bd=0, padx=16, pady=8)
            card.pack(side="left", padx=8)
            tk.Label(card, text=str(value), font=("Arial", 22, "bold"),
                     bg=self.COLORS["card"], fg=color).pack()
            tk.Label(card, text=label, font=("Arial", 9),
                     bg=self.COLORS["card"], fg=self.COLORS["muted"]).pack()

    def _build_main_content(self):
        content = tk.Frame(self, bg=self.COLORS["bg"])
        content.pack(fill="both", expand=True, padx=10, pady=5)

        # LEFT PANEL: Action Panel
        left = tk.Frame(content, bg=self.COLORS["surface"], width=280,
                        padx=15, pady=15)
        left.pack(side="left", fill="y", padx=(0, 5))
        left.pack_propagate(False)
        self._build_action_panel(left)

        # RIGHT PANEL: Data Table
        right = tk.Frame(content, bg=self.COLORS["surface"], padx=10, pady=10)
        right.pack(side="left", fill="both", expand=True)
        self._build_table_panel(right)

    def _build_action_panel(self, parent):
        """Panel kiri: tombol aksi utama"""
        tk.Label(parent, text="📥 IMPORT DATA", font=("Arial", 11, "bold"),
                 bg=self.COLORS["surface"], fg=self.COLORS["text"]).pack(anchor="w", pady=(0, 8))

        self._btn(parent, "📂 Import dari CSV", self.import_csv, self.COLORS["accent"])
        self._btn(parent, "📊 Import dari Excel", self.import_excel, self.COLORS["accent"])

        ttk.Separator(parent, orient="horizontal").pack(fill="x", pady=12)

        tk.Label(parent, text="✏️ INPUT MANUAL", font=("Arial", 11, "bold"),
                 bg=self.COLORS["surface"], fg=self.COLORS["text"]).pack(anchor="w", pady=(0, 8))
        self._build_manual_form(parent)

        ttk.Separator(parent, orient="horizontal").pack(fill="x", pady=12)

        tk.Label(parent, text="📤 EXPORT DATA", font=("Arial", 11, "bold"),
                 bg=self.COLORS["surface"], fg=self.COLORS["text"]).pack(anchor="w", pady=(0, 8))

        self.export_var = tk.StringVar(value=CONFIG["export_formats"][0])
        ddl = ttk.Combobox(parent, textvariable=self.export_var,
                           values=CONFIG["export_formats"], state="readonly", width=24)
        ddl.pack(fill="x", pady=(0, 6))
        self._btn(parent, "💾 Export Sekarang", self.export_data, self.COLORS["accent2"])

    def _build_manual_form(self, parent):
        """Form input manual untuk non-IT"""
        fields = [
            ("nama", "Nama Lengkap *"),
            ("email", "Email"),
            ("telepon", "No. Telepon / WA"),
            ("perusahaan", "Perusahaan / Instansi"),
        ]
        self.form_vars = {}
        for key, label in fields:
            tk.Label(parent, text=label, font=("Arial", 9),
                     bg=self.COLORS["surface"], fg=self.COLORS["muted"]).pack(anchor="w")
            var = tk.StringVar()
            entry = tk.Entry(parent, textvariable=var,
                             bg=self.COLORS["card"], fg=self.COLORS["text"],
                             insertbackground="white", relief="flat",
                             font=("Arial", 10))
            entry.pack(fill="x", pady=(0, 6), ipady=5)
            self.form_vars[key] = var

        # Kategori dropdown
        tk.Label(parent, text="Kategori", font=("Arial", 9),
                 bg=self.COLORS["surface"], fg=self.COLORS["muted"]).pack(anchor="w")
        self.form_vars["kategori"] = tk.StringVar(value="Prospect")
        cat_ddl = ttk.Combobox(parent, textvariable=self.form_vars["kategori"],
                               values=["Prospect", "Customer", "Partner", "VIP"],
                               state="readonly", width=24)
        cat_ddl.pack(fill="x", pady=(0, 6))
        self._btn(parent, "➕ Simpan Data", self.save_manual, "#8B5CF6")

    def _build_table_panel(self, parent):
        """Panel kanan: tabel data CRM + search"""
        # Search bar
        search_frame = tk.Frame(parent, bg=self.COLORS["surface"])
        search_frame.pack(fill="x", pady=(0, 8))

        tk.Label(search_frame, text="🔍", bg=self.COLORS["surface"],
                 fg=self.COLORS["muted"], font=("Arial", 12)).pack(side="left")
        self.search_var = tk.StringVar()
        self.search_var.trace("w", self._on_search)
        search_entry = tk.Entry(search_frame, textvariable=self.search_var,
                                bg=self.COLORS["card"], fg=self.COLORS["text"],
                                insertbackground="white", relief="flat",
                                font=("Arial", 11))
        search_entry.pack(side="left", fill="x", expand=True, ipady=6, padx=5)
        self._btn(search_frame, "🔄 Refresh", self._refresh_table,
                  self.COLORS["muted"], side="right", padx=0, pady=0)

        # Tabel
        cols = ("ID", "Nama", "Email", "Telepon", "Perusahaan", "Kategori", "Tanggal", "Status")
        self.tree = ttk.Treeview(parent, columns=cols, show="headings", height=22)

        widths = (40, 180, 180, 120, 160, 90, 130, 70)
        for col, w in zip(cols, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor="w")

        # Style tabel
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview",
                        background=self.COLORS["card"],
                        foreground=self.COLORS["text"],
                        fieldbackground=self.COLORS["card"],
                        rowheight=28, font=("Arial", 9))
        style.configure("Treeview.Heading",
                        background=self.COLORS["surface"],
                        foreground=self.COLORS["muted"],
                        font=("Arial", 9, "bold"))
        style.map("Treeview", background=[("selected", self.COLORS["accent"])])

        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _build_status_bar(self):
        self.status_var = tk.StringVar(value="✅ Engine siap digunakan. Selamat datang!")
        bar = tk.Frame(self, bg=self.COLORS["border"], pady=4)
        bar.pack(fill="x", side="bottom")
        tk.Label(bar, textvariable=self.status_var, font=("Arial", 9),
                 bg=self.COLORS["border"], fg=self.COLORS["muted"],
                 padx=12).pack(side="left")
        tk.Label(bar, text=f"Output: {CONFIG['output_dir']}",
                 font=("Arial", 8), bg=self.COLORS["border"],
                 fg=self.COLORS["muted"], padx=12).pack(side="right")

    # ── ACTIONS ──────────────────────────────
    def _btn(self, parent, text, cmd, color, side="top", padx=0, pady=3, fill="x"):
        btn = tk.Button(parent, text=text, command=cmd,
                        bg=color, fg="white", relief="flat",
                        font=("Arial", 10, "bold"),
                        cursor="hand2", activebackground=color,
                        padx=10, pady=7)
        btn.pack(fill=fill, pady=pady, side=side, padx=padx)
        return btn

    def import_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not path:
            return
        try:
            records = self.collector.from_csv(path)
            result = self.db.insert_many(records, sumber=f"CSV: {Path(path).name}")
            self._refresh_table()
            self._update_stats()
            self._set_status(f"✅ Import CSV selesai! Berhasil: {result['success']} | Gagal: {result['failed']}")
            messagebox.showinfo("Berhasil!", f"✅ {result['success']} data berhasil diimport!\n❌ {result['failed']} data gagal.")
        except Exception as e:
            messagebox.showerror("Error", f"Gagal import CSV:\n{e}")

    def import_excel(self):
        path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls")])
        if not path:
            return
        try:
            records = self.collector.from_excel(path)
            result = self.db.insert_many(records, sumber=f"Excel: {Path(path).name}")
            self._refresh_table()
            self._update_stats()
            self._set_status(f"✅ Import Excel selesai! Berhasil: {result['success']} | Gagal: {result['failed']}")
            messagebox.showinfo("Berhasil!", f"✅ {result['success']} data berhasil diimport!\n❌ {result['failed']} data gagal.")
        except Exception as e:
            messagebox.showerror("Error", f"Gagal import Excel:\n{e}")

    def save_manual(self):
        data = {k: v.get().strip() for k, v in self.form_vars.items()}
        if not data.get("nama"):
            messagebox.showwarning("Peringatan", "Nama wajib diisi!")
            return
        if self.db.insert_customer(data):
            for v in self.form_vars.values():
                v.set("")
            self.form_vars["kategori"].set("Prospect")
            self._refresh_table()
            self._update_stats()
            self._set_status(f"✅ Data '{data['nama']}' berhasil disimpan!")
        else:
            messagebox.showerror("Error", "Gagal menyimpan data.")

    def export_data(self):
        fmt = self.export_var.get()
        try:
            path = self.db.export(fmt)
            self._set_status(f"✅ Export selesai: {path}")
            if messagebox.askyesno("Export Berhasil", f"✅ File tersimpan di:\n{path}\n\nBuka folder sekarang?"):
                os.startfile(CONFIG["output_dir"]) if os.name == "nt" else os.system(f"xdg-open '{CONFIG['output_dir']}'")
        except Exception as e:
            messagebox.showerror("Error", f"Gagal export:\n{e}")

    def _refresh_table(self, *_):
        df = self.db.get_all()
        self._populate_table(df)

    def _on_search(self, *_):
        kw = self.search_var.get().strip()
        df = self.db.search(kw) if kw else self.db.get_all()
        self._populate_table(df)

    def _populate_table(self, df: pd.DataFrame):
        self.tree.delete(*self.tree.get_children())
        if df.empty:
            return
        for _, row in df.iterrows():
            self.tree.insert("", "end", values=(
                row.get("id", ""),
                row.get("nama", ""),
                row.get("email", ""),
                row.get("telepon", ""),
                row.get("perusahaan", ""),
                row.get("kategori", ""),
                str(row.get("tanggal", ""))[:16],
                row.get("status", ""),
            ))

    def _set_status(self, msg: str):
        self.status_var.set(msg)
        self.update_idletasks()


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    app = CRMApp()
    app.mainloop()