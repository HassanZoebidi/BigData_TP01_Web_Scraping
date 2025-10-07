import requests

# URL للموقع
url = "https://books.toscrape.com/"


response = requests.get(url)


print("Status:", response.status_code)


print(response.text[:1000])

# حفظ الكود في ملف
with open("page.html", "w", encoding="utf-8") as f:
    f.write(response.text)

print("✅ HTML saved to page.html")
