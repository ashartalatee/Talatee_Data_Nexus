import logging
import pandas as pd

logger = logging.getLogger(__name__)


def merge_data(invoice_df, bank_df):
    logger.info("Merging invoice and bank data")

    # merge berdasarkan customer + tanggal
    merged = pd.merge(
        invoice_df,
        bank_df,
        on=["customer", "transaction_date"],
        how="outer"
    )

    logger.info(f"Merged dataset shape: {merged.shape}")

    return merged


def apply_reconciliation_logic(df):
    logger.info("Applying reconciliation logic")

    def get_status(row):
        invoice_amt = row.get("invoice_amount")
        bank_amt = row.get("bank_amount")

        if pd.notnull(invoice_amt) and pd.notnull(bank_amt):
            if invoice_amt == bank_amt:
                return "Matched"
            else:
                return "Amount Mismatch"

        elif pd.notnull(invoice_amt) and pd.isnull(bank_amt):
            return "Missing Payment"

        elif pd.isnull(invoice_amt) and pd.notnull(bank_amt):
            return "Unmatched Bank Transaction"

        return "Unknown"

    df["status"] = df.apply(get_status, axis=1)

    return df