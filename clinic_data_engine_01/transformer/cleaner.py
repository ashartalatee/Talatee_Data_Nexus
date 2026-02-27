def remove_duplicates(data, key_fields):
    """
    Remove duplicate items based on combination of key_fields.
    Example:
        key_fields = ["quote", "author"]
    """

    seen = set()
    unique_data = []

    for item in data:
        # Create a tuple key from selected fields
        key = tuple(item.get(field) for field in key_fields)

        if key not in seen:
            seen.add(key)
            unique_data.append(item)

    return unique_data