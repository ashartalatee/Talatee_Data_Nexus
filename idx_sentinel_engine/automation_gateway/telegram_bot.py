import json
import os
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

class IDXTelegramBot:
    def __init__(self, orchestrator_callback, config_path: str = "core_system/config.json"):
        """
        orchestrator_callback: Fungsi eksekusi utama dari main.py yang akan dijalankan
                               ketika user mengetik perintah /proses di Telegram.
        """
        with open(config_path, "r") as f:
            self.config = json.load(f)
        self.run_engine_callback = orchestrator_callback

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update): return
        
        welcome_text = (
            "🤖 *IDX Capital Sentinel Engine Bot* 🤖\n\n"
            "Halo! Saya adalah asisten otomasi data saham Anda.\n"
            "Berikut perintah yang bisa Anda gunakan:\n"
            "➡️ `/scrap` - Otomatis download PDF terbaru dari web IDX\n"
            "➡️ `/proses` - Jalankan analisis Whale Tracking & buat laporan Excel\n"
            "➡️ `/status` - Cek status kondisi folder input/output"
        )
        await update.message.reply_text(welcome_text, parse_mode="Markdown")

    async def proses_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update): return
        
        await update.message.reply_text("⏳ Memulai pemrosesan data, mohon tunggu...")
        
        try:
            # Menjalankan alur engine utama lewat callback function
            success, message, report_path = self.run_engine_callback()
            
            if success:
                await update.message.reply_text(f"✅ *Proses Berhasil!*\n{message}", parse_mode="Markdown")
                # Jika file laporan excel ada, kirimkan langsung dokumennya ke Telegram user
                if report_path and os.path.exists(report_path):
                    with open(report_path, 'rb') as document:
                        await update.message.reply_document(document=document, filename=os.path.basename(report_path))
            else:
                await update.message.reply_text(f"❌ *Proses Gagal.*\nAlasan: {message}", parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"💥 Terjadi kesalahan internal pada sistem: {str(e)}")

    def _is_authorized(self, update: Update) -> bool:
        # Keamanan sistem: Memastikan hanya Chat ID terdaftar di config.json yang bisa mengakses bot ini
        allowed_ids = self.config["telegram"]["allowed_chat_ids"]
        user_id = update.effective_user.id
        
        if allowed_ids and user_id not in allowed_ids:
            print(f"[SECURITY] Akses ditolak untuk Chat ID: {user_id}")
            return False
        return True

    def start_polling(self):
        """Menjalankan bot Telegram di latar belakang."""
        token = self.config["telegram"]["bot_token"]
        if not self.config["telegram"]["enabled"] or token == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
            print("[TELEGRAM] Fitur Telegram Bot dinonaktifkan di config.json")
            return
            
        print("[TELEGRAM] Bot Telegram aktif dan mulai mendengarkan perintah...")
        application = Application.builder().token(token).build()
        
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("proses", self.proses_command))
        
        # Jalankan polling secara asinkron
        application.run_polling()