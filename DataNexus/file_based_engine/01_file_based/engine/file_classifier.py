def classify_files(files):

    csv_files = []
    excel_files = []
    pdf_files = []
    other_files = []

    for file in files:

        file_type = file["file_type"]

        if file_type == ".csv":
            csv_files.append(file)

        elif file_type in [".xlsx", ".xls"]:
            excel_files.append(file)

        elif file_type == ".pdf":
            pdf_files.append(file)

        else:
            other_files.append(file)

    return {
        "csv": sorted(csv_files, key=lambda x: x["file_name"]),
        "excel": sorted(excel_files, key=lambda x: x["file_name"]),
        "pdf": sorted(pdf_files, key=lambda x: x["file_name"]),
        "other": sorted(other_files, key=lambda x: x["file_name"])
    }