def normalize_text(text):
    if not text:
        return ""
    
    return (
        text.strip()
            .replace("\n", " ")
            .replace("\r", " ")
    )

def normalize_author(author):
    if not author:
        return ""
    
    return author.strip().lower()

def normalize_tags(tags):
    if not tags:
        return []
    
    return sorted([tag.strip().lower() for tag in tags])

def normalize_quotes_data(data):
    normalized = []

    for item in data:
        normalized_item = {
            "quote": normalize_text(item.get("quote")),
            "author": normalize_author(item.get("author")),
            "tags": normalize_tags(item.get("tags")),
        }
        normalized.append(normalized_item)


    return normalized

