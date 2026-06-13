# src/exceptions/errors.py

class RenameEngineException(Exception):
    """Base exception untuk seluruh ekosistem Batch Rename Engine."""
    pass

class FileCollisionError(RenameEngineException):
    """Dilempar ketika target nama file baru sudah ada di disk (mencegah overwrite data)."""
    pass

class TargetValidationError(RenameEngineException):
    """Dilempar ketika path direktori tidak valid, korup, atau tidak ditemukan."""
    pass

class OperationBlockedError(RenameEngineException):
    """Dilempar ketika OS menolak akses operasi (misal: File sedang terkunci oleh proses lain)."""
    pass