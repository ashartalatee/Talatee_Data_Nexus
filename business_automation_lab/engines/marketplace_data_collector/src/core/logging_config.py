import logging

class TalateeContextFilter(logging.Filter):
    """
    Filter infrastruktur untuk memastikan setiap log memiliki atribut 'run_id'.
    Mencegah sistem crash akibat KeyError saat format log memanggil %(run_id)s.
    """
    def filter(self, record):
        if not hasattr(record, 'run_id'):
            record.run_id = 'SYSTEM_CORE'
        return True

def setup_global_logging():
    logger = logging.getLogger("TalateeEngine")
    logger.setLevel(logging.INFO)

    # Cegah duplikasi handler log jika fungsi dipanggil ulang
    if not logger.handlers:
        # Format pencatatan log standar industri dengan run_id
        log_format = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(run_id)s] %(filename)s:%(lineno)d - %(message)s"
        )

        # 1. Handler untuk mencetak ke Console/Terminal
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        logger.addHandler(console_handler)

        # 2. Handler untuk menyimpan ke File Fisik di folder data/logs/
        try:
            file_handler = logging.FileHandler("data/logs/engine_execution.log", encoding="utf-8")
            file_handler.setFormatter(log_format)
            logger.addHandler(file_handler)
        except FileNotFoundError:
            # Toleransi jika folder data/logs belum terbuat di environment server tertentu
            pass

        # Sematkan filter pengaman context run_id
        context_filter = TalateeContextFilter()
        logger.addFilter(context_filter)