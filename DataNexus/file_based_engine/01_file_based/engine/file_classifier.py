def classify_files(files):

    classified = {
        "csv": [],
        "excel": [],
        "pdf": [],
        "other": []
    }

    for file in files:

        try:

            file_type = str(file.get("file_type", "")).lower()

            if file_type == ".csv":
                classified["csv"].append(file)

            elif file_type in [".xlsx", ".xls"]:
                classified["excel"].append(file)

            elif file_type == ".pdf":
                classified["pdf"].append(file)

            else:
                classified["other"].append(file)

        except Exception:
            classified["other"].append(file)

    # sort files for deterministic processing
    for key in classified:
        classified[key] = sorted(
            classified[key],
            key=lambda x: x.get("file_name", "")
        )

    return classified