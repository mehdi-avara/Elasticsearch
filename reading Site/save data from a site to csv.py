import requests
from bs4 import BeautifulSoup
import csv

url = "https://barghnews.com/"  

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

articles = soup.find_all('article', class_='div_akh')

csv_filename = 'extracted_data.csv'
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter='\t')
    csv_writer.writerow(["Topic", "Address", "Src", "Date"])

    for article in articles:
        topic = article.find('a', class_='title_akh').text.strip()
        address = article.find('a', class_='title_akh')['href']
        src = article.find('img', class_='div_akh_img')['src']
        date = article.find('a', class_='title_akh')['title'].split(' - ')[1].strip()

        csv_writer.writerow([topic, address, src, date])

print(f"Data has been saved to {csv_filename}")
