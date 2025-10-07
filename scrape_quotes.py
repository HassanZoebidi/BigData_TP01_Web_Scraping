import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://quotes.toscrape.com/"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

quotes = []


for q in soup.select("div.quote"):
    text = q.select_one("span.text").get_text(strip=True)
    author = q.select_one("small.author").get_text(strip=True)
    tags = [t.get_text() for t in q.select("div.tags a.tag")]
    quotes.append({
        "Quote": text,
        "Author": author,
        "Tags": ", ".join(tags)
    })


df = pd.DataFrame(quotes)
df.to_csv("quotes.csv", index=False, encoding="utf-8-sig")

print("✅ Saved", len(df), "quotes to quotes.csv")
