def validate_quote_item(item):
    """
    Validate single quote item.
    Return (is_valid, error_message)
    """

    if not isinstance(item, dict):
        return False, "item is not a dictionary"
    
    if not item.get("quote"):
        return False, "Missing author"
    
    if not item.get("author"):
        return False, "Missing author"
    
    if not isinstance(item.get("tags"), list):
        return False, "Tags is not a list"
    
    return True, None

def validate_quotes_data(data):
    valid_data = []
    errors = []

    for index, item in enumerate(data):
        is_valid, error = validate_quote_item(item)

        if is_valid:
            valid_data.append(item)
        else:
            errors.append({
                "index": index,
                "error": error,
                "item": item
            })

    return valid_data, errors