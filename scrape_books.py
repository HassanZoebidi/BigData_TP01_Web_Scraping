import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://books.toscrape.com/"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

books = []


for article in soup.select("article.product_pod"):
    title = article.h3.a["title"]
    price = article.select_one("p.price_color").get_text()
    availability = article.select_one("p.instock.availability").get_text(strip=True)
    rating_tag = article.select_one("p.star-rating")
    rating = rating_tag["class"][1] if rating_tag else "Unknown"

    books.append({
        "Title": title,
        "Price": price,
        "Availability": availability,
        "Rating": rating
    })


df = pd.DataFrame(books)
df.to_csv("books.csv", index=False, encoding="utf-8-sig")

print("✅ Saved", len(df), "books to books.csv")
