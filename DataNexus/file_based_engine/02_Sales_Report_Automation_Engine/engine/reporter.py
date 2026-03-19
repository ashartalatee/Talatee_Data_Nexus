import logging


def generate_report(df):

    logging.info("Starting report generation")

    output_path = "output/sales_report.xlsx"

    try:
        df.to_excel(output_path, index=False)

        logging.info(f"Report successfully saved to {output_path}")

    except Exception as e:
        logging.error(f"Failed to generate report: {e}")
        raise

    return output_path