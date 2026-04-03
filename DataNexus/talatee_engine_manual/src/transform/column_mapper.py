def apply_column_mapping(df, mapping, logger):
    """
    Apply column mapping from RAW → STANDARD schema.

    Features:
    - Normalize column names (lowercase, strip)
    - Only map existing columns
    - Avoid accidental overwrite
    - Detailed logging
    """

    if df is None or df.empty:
        logger.warning("[MAPPING] DataFrame kosong")
        return df

    if not mapping:
        logger.warning("[MAPPING] No mapping provided")
        return df

    # ========================
    # NORMALIZE COLUMN NAMES
    # ========================
    df.columns = [col.strip().lower() for col in df.columns]

    # ========================
    # FILTER VALID MAPPING
    # (hanya mapping yang ada di df)
    # ========================
    valid_mapping = {
        raw.strip().lower(): std
        for raw, std in mapping.items()
        if raw.strip().lower() in df.columns
    }

    if not valid_mapping:
        logger.warning("[MAPPING] No matching columns found in data")
        return df

    # ========================
    # DETECT POTENTIAL COLLISION
    # ========================
    collisions = [std for std in valid_mapping.values() if std in df.columns]
    if collisions:
        logger.warning(f"[MAPPING WARNING] Potential overwrite columns: {collisions}")

    # ========================
    # APPLY MAPPING
    # ========================
    df = df.rename(columns=valid_mapping)

    # ========================
    # LOGGING
    # ========================
    logger.info(f"[MAPPING] Applied mapping: {valid_mapping}")
    logger.info(f"[COLUMNS AFTER] {df.columns.tolist()}")

    return df