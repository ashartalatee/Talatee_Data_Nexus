from bs4 import BeautifulSoup

def parse_quotes(html):
    soup = BeautifulSoup(html, "html.parser")
    quotes_data = []

    quotes = soup.find_all("div", class_="quote")

    for q in quotes:
        text_tag = q.find("span", class_="text")
        author_tag = q.find("small", class_="author")
        tag_elements = q.find_all("a", class_="tag")

        if not text_tag or not author_tag:
            continue  # skip kalau struktur aneh

        text = text_tag.get_text(strip=True)
        author = author_tag.get_text(strip=True)
        tags = [tag.get_text(strip=True) for tag in tag_elements]

        quotes_data.append({
            "quote": text,
            "author": author,
            "tags": tags
        })

    return quotes_data