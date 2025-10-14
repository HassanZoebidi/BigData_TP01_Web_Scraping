import requests
from bs4 import BeautifulSoup
import pandas as pd
import time


base_url = "https://books.toscrape.com/catalogue/page-{}.html"

books = []


for page in range(1, 51):
    url = base_url.format(page)
    response = requests.get(url)

    if response.status_code != 200:
        print(f"⚠️ Skipping page {page} (status {response.status_code})")
        continue

    soup = BeautifulSoup(response.text, "html.parser")

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
            "Rating": rating,
            "Page": page
        })

    print(f"✅ Page {page} scraped — total books: {len(books)}")
    time.sleep(0.5)  


df = pd.DataFrame(books)
df.to_csv("books_all_pages.csv", index=False, encoding="utf-8-sig")

print("📘 Done! Saved", len(df), "books to books_all_pages.csv")
