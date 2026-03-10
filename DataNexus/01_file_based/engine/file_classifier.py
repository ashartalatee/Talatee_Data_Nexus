class FileClassifier:

    def __init__(self, files_metadata):
        self.files_metadata = files_metadata

    def classify(self):

        csv_files = []
        excel_files = []
        pdf_files = []

        for file in self.files_metadata:

            file_type = file["file_type"].lower()

            if file_type == ".csv":
                csv_files.append(file)

            elif file_type in [".xlsx", ".xls"]:
                excel_files.append(file)

            elif file_type == ".pdf":
                pdf_files.append(file)

        return {
            "csv": csv_files,
            "excel": excel_files,
            "pdf": pdf_files
        }