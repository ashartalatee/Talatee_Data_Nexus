import logging

logger = logging.getLogger(__name__)


def extract_customer(description):
    """
    Extract customer name from bank description
    contoh: 'payment customer a' → 'Customer A'
    """
    try:
        if "customer" in description:
            return description.split("customer")[-1].strip().title()
        return "Unknown"
    except Exception:
        return "Unknown"


def transform_bank_data(df):
    logger.info("Transforming bank data")

    # extract customer
    df["customer"] = df["description"].apply(extract_customer)

    # standardisasi kolom
    df = df.rename(columns={
        "date": "transaction_date",
        "amount": "bank_amount"
    })

    return df


def transform_invoice_data(df):
    logger.info("Transforming invoice data")

    df = df.rename(columns={
        "amount": "invoice_amount",
        "invoice_date": "transaction_date"
    })

    return df