import requests
from bs4 import BeautifulSoup

url = "https://barghnews.com/" 

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

articles = soup.find_all('article', class_='div_akh')

print("Topic\tAddress\tSrc\tDate")
num =0
for article in articles:
    num=num+1
    print(num)
    topic = article.find('a', class_='title_akh').text.strip()
    address = article.find('a', class_='title_akh')['href']
    src = article.find('img', class_='div_akh_img')['src']
    date = article.find('a', class_='title_akh')['title'].split(' - ')[1].strip()
    print("\n\n\n\n\n\n\n")

    print(f"{topic}\t{address}\t{src}\t{date}")
