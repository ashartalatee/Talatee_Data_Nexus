# src/exceptions/errors.py

class OrganizerEngineException(Exception):
    """Base exception untuk seluruh ekosistem File Organizer Engine."""
    pass

class StorageFullError(OrganizerEngineException):
    """Dilempar jika kapasitas penyimpanan hard drive tujuan tidak memadai."""
    pass

class PathBlockedError(OrganizerEngineException):
    """Dilempar jika file target sedang dikunci oleh sistem atau hak akses ditolak."""
    pass

class TargetDirectoryConflictError(OrganizerEngineException):
    """Dilempar jika terjadi duplikasi file atau struktur direktori yang rusak saat mutasi."""
    pass