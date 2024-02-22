import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

url = "https://barghnews.com/" 

es = Elasticsearch('http://localhost:9200') 

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

articles = soup.find_all('article', class_='div_akh')

for article in articles:
    topic = article.find('a', class_='title_akh').text.strip()
    address = article.find('a', class_='title_akh')['href']
    src = article.find('img', class_='div_akh_img')['src']
    date = article.find('a', class_='title_akh')['title'].split(' - ')[1].strip()

    data = {
        'topic': topic,
        'address': address,
        'src': src,
        'date': date,
    }

    index_name = 'news'
    es.index(index=index_name, body=data)

    print(f"Data indexed in Elasticsearch for topic: {topic}")
