"""
app.py
======
PDF EXTRACTOR ENGINE — Desktop App (GUI)
Bagian dari Python Business Automation Engine (Engine #1 COLLECT, tool #5).

Aplikasi ini dibuat supaya orang NON-IT bisa langsung pakai:
    1. Buka aplikasi (double-click .exe)
    2. Pilih file PDF
    3. Pilih apa saja yang mau diambil (teks / tabel / gambar / metadata)
    4. Pilih folder simpan
    5. Klik "PROSES SEKARANG"
    6. Selesai -> buka folder hasil

Tidak perlu tahu coding, tidak perlu buka terminal.

Cara build jadi .exe: lihat build_exe.bat / README.md
"""

import os
import queue
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from extractor import PDFExtractorEngine

APP_TITLE = "PDF Extractor Engine"
APP_VERSION = "1.0.0"
BG_DARK = "#0b1220"
BG_PANEL = "#121b2e"
ACCENT = "#f5a623"
ACCENT_BLUE = "#2f80ed"
TEXT_LIGHT = "#e8edf5"
TEXT_MUTED = "#93a1b8"
GREEN_OK = "#27ae60"


class PDFExtractorApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f"{APP_TITLE} v{APP_VERSION}")
        self.root.geometry("760x640")
        self.root.minsize(680, 580)
        self.root.configure(bg=BG_DARK)

        self.pdf_path = tk.StringVar()
        self.output_dir = tk.StringVar()
        self.opt_text = tk.BooleanVar(value=True)
        self.opt_tables = tk.BooleanVar(value=True)
        self.opt_images = tk.BooleanVar(value=True)
        self.opt_metadata = tk.BooleanVar(value=True)

        self.log_queue: queue.Queue = queue.Queue()
        self.is_running = False

        self._build_ui()
        self.root.after(100, self._poll_log_queue)

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def _build_ui(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("TCheckbutton", background=BG_PANEL, foreground=TEXT_LIGHT, font=("Segoe UI", 10))
        style.map("TCheckbutton", background=[("active", BG_PANEL)])
        style.configure("Horizontal.TProgressbar", troughcolor=BG_PANEL, background=ACCENT, thickness=14)

        # ---- Header -----------------------------------------------------
        header = tk.Frame(self.root, bg=BG_DARK)
        header.pack(fill="x", padx=24, pady=(20, 10))

        tk.Label(
            header, text="📄 PDF EXTRACTOR ENGINE", font=("Segoe UI", 18, "bold"),
            bg=BG_DARK, fg=TEXT_LIGHT,
        ).pack(anchor="w")
        tk.Label(
            header,
            text="Ambil teks, tabel, gambar & metadata dari PDF — otomatis, tanpa coding.",
            font=("Segoe UI", 10), bg=BG_DARK, fg=TEXT_MUTED,
        ).pack(anchor="w", pady=(2, 0))

        # ---- Step 1: pilih file ------------------------------------------
        step1 = self._panel("1) Pilih File PDF")
        row = tk.Frame(step1, bg=BG_PANEL)
        row.pack(fill="x", pady=(6, 0))
        entry = tk.Entry(row, textvariable=self.pdf_path, font=("Segoe UI", 10),
                          bg="#0e1727", fg=TEXT_LIGHT, insertbackground=TEXT_LIGHT, relief="flat")
        entry.pack(side="left", fill="x", expand=True, ipady=6, padx=(0, 8))
        self._button(row, "Pilih PDF...", self.pick_pdf, ACCENT_BLUE).pack(side="left")

        # ---- Step 2: pilih fitur ------------------------------------------
        step2 = self._panel("2) Pilih Apa Yang Mau Diambil")
        grid = tk.Frame(step2, bg=BG_PANEL)
        grid.pack(fill="x", pady=(6, 0))
        ttk.Checkbutton(grid, text="📝  Teks", variable=self.opt_text).grid(row=0, column=0, sticky="w", padx=(0, 24), pady=4)
        ttk.Checkbutton(grid, text="📊  Tabel (ke Excel)", variable=self.opt_tables).grid(row=0, column=1, sticky="w", padx=(0, 24), pady=4)
        ttk.Checkbutton(grid, text="🖼️  Gambar", variable=self.opt_images).grid(row=1, column=0, sticky="w", padx=(0, 24), pady=4)
        ttk.Checkbutton(grid, text="ℹ️  Metadata", variable=self.opt_metadata).grid(row=1, column=1, sticky="w", padx=(0, 24), pady=4)

        # ---- Step 3: pilih folder output ------------------------------------------
        step3 = self._panel("3) Pilih Folder Simpan Hasil")
        row3 = tk.Frame(step3, bg=BG_PANEL)
        row3.pack(fill="x", pady=(6, 0))
        entry3 = tk.Entry(row3, textvariable=self.output_dir, font=("Segoe UI", 10),
                           bg="#0e1727", fg=TEXT_LIGHT, insertbackground=TEXT_LIGHT, relief="flat")
        entry3.pack(side="left", fill="x", expand=True, ipady=6, padx=(0, 8))
        self._button(row3, "Pilih Folder...", self.pick_output_dir, ACCENT_BLUE).pack(side="left")

        # ---- Tombol proses ------------------------------------------
        action_row = tk.Frame(self.root, bg=BG_DARK)
        action_row.pack(fill="x", padx=24, pady=(4, 8))
        self.run_button = self._button(action_row, "🚀 PROSES SEKARANG", self.start_extraction, ACCENT, big=True)
        self.run_button.pack(side="left")
        self.open_folder_btn = self._button(action_row, "📂 Buka Folder Hasil", self.open_output_folder, "#2d3b52")
        self.open_folder_btn.pack(side="left", padx=(10, 0))
        self.open_folder_btn.config(state="disabled")

        self.progress = ttk.Progressbar(self.root, mode="indeterminate", style="Horizontal.TProgressbar")
        self.progress.pack(fill="x", padx=24, pady=(0, 10))

        # ---- Log area ------------------------------------------
        log_panel = self._panel("Log Proses")
        self.log_text = tk.Text(
            log_panel, height=10, bg="#0e1727", fg="#c9d6e8", relief="flat",
            font=("Consolas", 9), wrap="word",
        )
        self.log_text.pack(fill="both", expand=True, pady=(6, 0))
        self.log_text.configure(state="disabled")

        footer = tk.Label(
            self.root, text=f"{APP_TITLE} v{APP_VERSION}  •  Bagian dari Python Business Automation Engine",
            font=("Segoe UI", 8), bg=BG_DARK, fg=TEXT_MUTED,
        )
        footer.pack(pady=(4, 10))

    def _panel(self, title: str) -> tk.Frame:
        outer = tk.Frame(self.root, bg=BG_DARK)
        outer.pack(fill="x" if title != "Log Proses" else "both", expand=(title == "Log Proses"),
                   padx=24, pady=6)
        tk.Label(outer, text=title, font=("Segoe UI", 10, "bold"), bg=BG_DARK, fg=ACCENT).pack(anchor="w")
        panel = tk.Frame(outer, bg=BG_PANEL, padx=14, pady=10)
        panel.pack(fill="both" if title == "Log Proses" else "x", expand=(title == "Log Proses"), pady=(4, 0))
        return panel

    def _button(self, parent, text, command, color, big=False):
        return tk.Button(
            parent, text=text, command=command, bg=color, fg="white" if color != ACCENT else "#1a1200",
            activebackground=color, relief="flat", font=("Segoe UI", 11 if big else 9, "bold" if big else "normal"),
            padx=16 if big else 10, pady=10 if big else 6, cursor="hand2",
        )

    # ------------------------------------------------------------------
    # ACTIONS
    # ------------------------------------------------------------------
    def pick_pdf(self):
        path = filedialog.askopenfilename(title="Pilih file PDF", filetypes=[("File PDF", "*.pdf")])
        if path:
            self.pdf_path.set(path)
            if not self.output_dir.get():
                default_out = os.path.join(os.path.dirname(path), "hasil_ekstraksi")
                self.output_dir.set(default_out)

    def pick_output_dir(self):
        path = filedialog.askdirectory(title="Pilih folder untuk menyimpan hasil")
        if path:
            self.output_dir.set(path)

    def open_output_folder(self):
        path = self.output_dir.get()
        if not path or not os.path.isdir(path):
            return
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.run(["open", path])
        else:
            subprocess.run(["xdg-open", path])

    def _log(self, msg: str):
        self.log_queue.put(msg)

    def _poll_log_queue(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.configure(state="normal")
                self.log_text.insert("end", msg + "\n")
                self.log_text.see("end")
                self.log_text.configure(state="disabled")
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log_queue)

    def start_extraction(self):
        if self.is_running:
            return
        pdf = self.pdf_path.get().strip()
        out_dir = self.output_dir.get().strip()

        if not pdf:
            messagebox.showwarning(APP_TITLE, "Pilih file PDF dulu ya.")
            return
        if not os.path.isfile(pdf):
            messagebox.showerror(APP_TITLE, "File PDF tidak ditemukan.")
            return
        if not out_dir:
            messagebox.showwarning(APP_TITLE, "Pilih folder untuk menyimpan hasil dulu ya.")
            return
        if not any([self.opt_text.get(), self.opt_tables.get(), self.opt_images.get(), self.opt_metadata.get()]):
            messagebox.showwarning(APP_TITLE, "Pilih minimal satu jenis data yang mau diambil.")
            return

        self.is_running = True
        self.run_button.config(state="disabled", text="⏳ Memproses...")
        self.open_folder_btn.config(state="disabled")
        self.progress.start(12)
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

        thread = threading.Thread(target=self._run_extraction_thread, args=(pdf, out_dir), daemon=True)
        thread.start()

    def _run_extraction_thread(self, pdf: str, out_dir: str):
        try:
            engine = PDFExtractorEngine(pdf, log=self._log)
            result = engine.extract_all(
                out_dir,
                do_text=self.opt_text.get(),
                do_tables=self.opt_tables.get(),
                do_images=self.opt_images.get(),
                do_metadata=self.opt_metadata.get(),
            )
            self._log("")
            self._log("✅ SELESAI!")
            self._log(f"   Halaman diproses : {result.pages}")
            if result.text_saved:
                self._log(f"   Teks             : {result.text_char_count} karakter")
            if result.tables_saved:
                self._log(f"   Tabel            : {result.tables_found} tabel -> file Excel")
            if result.images_saved:
                self._log(f"   Gambar           : {result.images_found} gambar")
            if result.errors:
                self._log(f"   ⚠️ Ada {len(result.errors)} peringatan, cek detail di atas.")
            self.root.after(0, self._on_finished, True, None)
        except Exception as e:  # noqa: BLE001
            self._log(f"❌ ERROR: {e}")
            self.root.after(0, self._on_finished, False, str(e))

    def _on_finished(self, success: bool, error_msg):
        self.is_running = False
        self.progress.stop()
        self.run_button.config(state="normal", text="🚀 PROSES SEKARANG")
        self.open_folder_btn.config(state="normal")
        if success:
            messagebox.showinfo(APP_TITLE, "Ekstraksi selesai! Klik 'Buka Folder Hasil' untuk melihat file-nya.")
        else:
            messagebox.showerror(APP_TITLE, f"Terjadi kesalahan:\n{error_msg}")


def main():
    root = tk.Tk()
    app = PDFExtractorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
