"""
File Watcher Engine — GUI for Non-IT Users
============================================
Click a folder. Choose an action. Press Start. Done.

No coding needed. Designed for business users.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import sys
import os

# Allow running directly or from parent folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from engine import FileWatcherEngine, WatcherConfig, FileEvent


# ── Colors & Fonts ────────────────────────────────────────────────────────────

BG        = "#0f1117"
CARD      = "#1a1d27"
ACCENT    = "#4f8ef7"
SUCCESS   = "#34c77b"
WARNING   = "#f5a623"
DANGER    = "#e05252"
TEXT      = "#e8eaf0"
MUTED     = "#7a7e94"
FONT_MAIN = ("Segoe UI", 11)
FONT_BOLD = ("Segoe UI", 11, "bold")
FONT_H1   = ("Segoe UI", 18, "bold")
FONT_H2   = ("Segoe UI", 13, "bold")
FONT_MONO = ("Consolas", 10)


# ── Main App ──────────────────────────────────────────────────────────────────

class FileWatcherApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("File Watcher Engine  ·  Collect Engine #9")
        self.geometry("820x680")
        self.minsize(700, 580)
        self.configure(bg=BG)
        self.resizable(True, True)

        self._engine: FileWatcherEngine = None
        self._running = False
        self._event_count = 0

        self._build_ui()
        self._set_defaults()

    # ── UI Builder ────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Header ────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg=CARD, pady=16)
        hdr.pack(fill="x")
        tk.Label(hdr, text="📂  File Watcher Engine",
                 bg=CARD, fg=TEXT, font=FONT_H1).pack(side="left", padx=24)
        tk.Label(hdr, text="Collect Engine #9  ·  Python Business Automation",
                 bg=CARD, fg=MUTED, font=FONT_MAIN).pack(side="left", padx=4)

        # Status badge (right)
        self._status_lbl = tk.Label(hdr, text="● Idle",
                                    bg=CARD, fg=MUTED, font=FONT_BOLD)
        self._status_lbl.pack(side="right", padx=24)

        # ── Config Panel ──────────────────────────────────────────────
        cfg = tk.Frame(self, bg=BG, padx=20, pady=16)
        cfg.pack(fill="x")

        # Watch Folder
        self._mk_row(cfg, 0, "📁  Folder to Watch",
                     help_text="Folder yang akan dipantau otomatis")
        self._watch_var = tk.StringVar()
        row0 = tk.Frame(cfg, bg=BG)
        row0.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        cfg.columnconfigure(0, weight=1)
        tk.Entry(row0, textvariable=self._watch_var, bg=CARD, fg=TEXT,
                 insertbackground=TEXT, relief="flat",
                 font=FONT_MAIN, width=60).pack(side="left", fill="x",
                                                 expand=True, ipady=8, padx=(0, 8))
        tk.Button(row0, text="Browse…", command=self._pick_watch,
                  bg=ACCENT, fg="white", relief="flat",
                  font=FONT_BOLD, padx=12, cursor="hand2").pack(side="right")

        # Output Folder
        self._mk_row(cfg, 2, "📤  Output / Destination Folder",
                     help_text="Tempat file hasil dipindah/disalin")
        self._output_var = tk.StringVar()
        row1 = tk.Frame(cfg, bg=BG)
        row1.grid(row=3, column=0, sticky="ew", pady=(0, 12))
        tk.Entry(row1, textvariable=self._output_var, bg=CARD, fg=TEXT,
                 insertbackground=TEXT, relief="flat",
                 font=FONT_MAIN, width=60).pack(side="left", fill="x",
                                                 expand=True, ipady=8, padx=(0, 8))
        tk.Button(row1, text="Browse…", command=self._pick_output,
                  bg=CARD, fg=TEXT, relief="flat",
                  font=FONT_BOLD, padx=12, cursor="hand2").pack(side="right")

        # Action + Extensions row
        opts = tk.Frame(cfg, bg=BG)
        opts.grid(row=4, column=0, sticky="ew", pady=(0, 16))

        # Action dropdown
        tk.Label(opts, text="⚡  Aksi", bg=BG, fg=MUTED, font=FONT_MAIN).pack(
            side="left")
        self._action_var = tk.StringVar(value="copy")
        action_menu = ttk.Combobox(opts, textvariable=self._action_var,
                                   values=["copy", "move", "log_only"],
                                   state="readonly", width=14,
                                   font=FONT_MAIN)
        action_menu.pack(side="left", padx=(6, 24))

        # Extensions
        tk.Label(opts, text="🔍  Filter Ekstensi (kosong = semua)",
                 bg=BG, fg=MUTED, font=FONT_MAIN).pack(side="left")
        self._ext_var = tk.StringVar(value="")
        tk.Entry(opts, textvariable=self._ext_var, bg=CARD, fg=TEXT,
                 insertbackground=TEXT, relief="flat",
                 font=FONT_MAIN, width=22).pack(side="left", ipady=6, padx=6)

        # Interval
        tk.Label(opts, text="⏱  Interval (detik)",
                 bg=BG, fg=MUTED, font=FONT_MAIN).pack(side="left", padx=(16, 0))
        self._interval_var = tk.IntVar(value=5)
        tk.Spinbox(opts, from_=1, to=60, textvariable=self._interval_var,
                   bg=CARD, fg=TEXT, relief="flat",
                   font=FONT_MAIN, width=5).pack(side="left", padx=6)

        # ── Buttons ───────────────────────────────────────────────────
        btns = tk.Frame(self, bg=BG, padx=20, pady=4)
        btns.pack(fill="x")

        self._start_btn = tk.Button(btns, text="▶  Start Watching",
                                    command=self._start,
                                    bg=SUCCESS, fg="white", relief="flat",
                                    font=FONT_BOLD, padx=20, pady=10,
                                    cursor="hand2")
        self._start_btn.pack(side="left", padx=(0, 8))

        self._stop_btn = tk.Button(btns, text="■  Stop",
                                   command=self._stop,
                                   bg=CARD, fg=MUTED, relief="flat",
                                   font=FONT_BOLD, padx=20, pady=10,
                                   state="disabled", cursor="hand2")
        self._stop_btn.pack(side="left", padx=(0, 8))

        tk.Button(btns, text="🗑  Clear Log",
                  command=self._clear_log,
                  bg=CARD, fg=MUTED, relief="flat",
                  font=FONT_BOLD, padx=16, pady=10,
                  cursor="hand2").pack(side="left")

        # Stats
        self._stats_lbl = tk.Label(btns, text="Events: 0  |  Created: 0  |  Modified: 0  |  Deleted: 0",
                                   bg=BG, fg=MUTED, font=FONT_MAIN)
        self._stats_lbl.pack(side="right", padx=8)

        # ── Activity Log ──────────────────────────────────────────────
        log_frame = tk.Frame(self, bg=BG, padx=20, pady=8)
        log_frame.pack(fill="both", expand=True)

        tk.Label(log_frame, text="📋  Activity Log",
                 bg=BG, fg=TEXT, font=FONT_H2).pack(anchor="w", pady=(0, 6))

        # Table header
        cols = ("Time", "Event", "File Name", "Size (KB)")
        self._tree = ttk.Treeview(log_frame, columns=cols, show="headings",
                                  height=14)
        widths = [140, 90, 400, 90]
        for col, w in zip(cols, widths):
            self._tree.heading(col, text=col)
            self._tree.column(col, width=w, minwidth=60)

        # Style the treeview
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Treeview",
                        background=CARD, foreground=TEXT,
                        rowheight=26, fieldbackground=CARD,
                        font=FONT_MONO, borderwidth=0)
        style.configure("Treeview.Heading",
                        background=BG, foreground=MUTED,
                        font=FONT_BOLD, relief="flat")
        style.map("Treeview", background=[("selected", ACCENT)])

        sb = tk.Scrollbar(log_frame, orient="vertical",
                          command=self._tree.yview, bg=CARD)
        self._tree.configure(yscrollcommand=sb.set)

        self._tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # ── Footer ────────────────────────────────────────────────────
        footer = tk.Frame(self, bg=CARD, pady=8)
        footer.pack(fill="x", side="bottom")
        tk.Label(footer,
                 text="💡 Tidak perlu coding · Log tersimpan di folder logs/ · "
                      "github.com/[username]/file-watcher-engine",
                 bg=CARD, fg=MUTED, font=("Segoe UI", 9)).pack()

    def _mk_row(self, parent, row, text, help_text=""):
        lbl_frame = tk.Frame(parent, bg=BG)
        lbl_frame.grid(row=row, column=0, sticky="w", pady=(0, 4))
        tk.Label(lbl_frame, text=text, bg=BG, fg=TEXT, font=FONT_BOLD).pack(
            side="left")
        if help_text:
            tk.Label(lbl_frame, text=f"  —  {help_text}",
                     bg=BG, fg=MUTED, font=FONT_MAIN).pack(side="left")

    # ── Folder Pickers ────────────────────────────────────────────────────────

    def _pick_watch(self):
        d = filedialog.askdirectory(title="Pilih folder yang ingin dipantau")
        if d:
            self._watch_var.set(d)

    def _pick_output(self):
        d = filedialog.askdirectory(title="Pilih folder tujuan output")
        if d:
            self._output_var.set(d)

    # ── Engine Controls ───────────────────────────────────────────────────────

    def _start(self):
        watch = self._watch_var.get().strip()
        if not watch:
            messagebox.showwarning("Folder kosong",
                                   "Pilih folder yang ingin dipantau dulu.")
            return

        out = self._output_var.get().strip() or "output"
        raw_ext = self._ext_var.get().strip()
        exts = [e.strip().lower() for e in raw_ext.split(",")
                if e.strip()] if raw_ext else ["*"]

        config = WatcherConfig(
            watch_folder           = watch,
            output_folder          = out,
            action                 = self._action_var.get(),
            file_extensions        = exts,
            check_interval_seconds = self._interval_var.get(),
        )

        try:
            self._engine = FileWatcherEngine(config, on_event=self._on_event)
            self._engine.start()
        except FileNotFoundError as e:
            messagebox.showerror("Folder tidak ditemukan", str(e))
            return

        self._running = True
        self._set_status("running")
        self._start_btn.config(state="disabled")
        self._stop_btn.config(state="normal", bg=DANGER, fg="white")

    def _stop(self):
        if self._engine:
            self._engine.stop()
            self._engine = None
        self._running = False
        self._set_status("idle")
        self._start_btn.config(state="normal")
        self._stop_btn.config(state="disabled", bg=CARD, fg=MUTED)

    def _on_event(self, event: FileEvent):
        """Called from engine thread → schedule UI update on main thread."""
        self.after(0, self._add_row, event)

    def _add_row(self, event: FileEvent):
        icons = {"CREATED": "✅", "MODIFIED": "🔄", "DELETED": "🗑️"}
        icon  = icons.get(event.event_type, "")
        self._tree.insert("", 0, values=(
            event.detected_at,
            f"{icon} {event.event_type}",
            event.file_name,
            event.file_size_kb,
        ))
        self._event_count += 1
        self._update_stats()

    def _clear_log(self):
        for item in self._tree.get_children():
            self._tree.delete(item)
        self._event_count = 0
        self._update_stats()

    def _update_stats(self):
        if self._engine:
            s = self._engine.get_stats()
            self._stats_lbl.config(
                text=f"Events: {self._event_count}  |  "
                     f"✅ Created: {s['created']}  |  "
                     f"🔄 Modified: {s['modified']}  |  "
                     f"🗑️ Deleted: {s['deleted']}"
            )

    def _set_status(self, state: str):
        if state == "running":
            self._status_lbl.config(text="● Watching…", fg=SUCCESS)
        else:
            self._status_lbl.config(text="● Idle", fg=MUTED)

    def _set_defaults(self):
        self._output_var.set("output")


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = FileWatcherApp()
    app.mainloop()
