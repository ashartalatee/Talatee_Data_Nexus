class ExcelEngineException(Exception):
    """Base exception untuk seluruh error yang terjadi pada Excel Merger Engine."""
    pass

class SchemaMismatchError(ExcelEngineException):
    """Dilempar ketika file Excel yang diidentifikasi tidak cocok dengan skema wajib."""
    pass

class CorruptedFileError(ExcelEngineException):
    """Dilempar ketika file Excel rusak atau tidak dapat dibaca oleh driver."""
    pass