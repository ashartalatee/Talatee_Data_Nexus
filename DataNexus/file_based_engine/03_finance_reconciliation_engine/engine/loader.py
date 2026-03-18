import pandas as pd
import logging
from config.settings import BANK_FILE, INVOICE_FILE

logger = logging.getLogger(__name__)

def load_bank_data():
    try:
        logger.info(f"Loading bank data from {BANK_FILE}")
        df = pd.read_csv(BANK_FILE)

        logger.info(f"Bank data loaded: {df.shape[0]} rows")
        return df
    
    except FileExistsError:
        logger.error("Bank file not found!")
        raise
    
    except Exception as e:
        logger.error(f"Error loading bank data: {e}")
        raise

def load_invoice_data():
    try:
        logger.info(f"Loading invoice data from {INVOICE_FILE}")
        df = pd.read_csv(INVOICE_FILE)

        logger.info(f"Invoice data loaded: {df.shape[0]} rows")
        return df
    
    except FileExistsError:
        logger.error("Invoice file not found!")
        raise

    except Exception as e:
        logger.error(f"Error loading invoice data: {e}")
        raise