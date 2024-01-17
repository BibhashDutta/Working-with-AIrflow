import pandas as pd
import requests
from bs4 import BeautifulSoup

def run_imdb_etl():
        # URL of the page containing reviews (replace with the actual URL)
    url = "https://www.imdb.com/title/tt12915716/reviews/?ref_=tt_ql_2"

    # Send a GET request and parse the HTML content
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    def transform(soup):
        divs=soup.find_all("div", class_="review-container")
        for item in divs:
            review_date=item.find('span', class_= 'review-date').get_text()
            review_rating = item.find('span', class_= 'rating-other-user-rating').find('span').get_text()
            review_title = item.find('a', class_= 'title').get_text()
            review_text = item.find('div', class_= 'text show-more__control').get_text()
       
            review={
                'review_date':review_date,
                'review_rating(out of 10)':review_rating,
                'review_title':review_title,
                'review_text':review_text
            }
            review_list.append(review)
        return

    review_list = []

    transform(soup)
    df=pd.DataFrame(review_list)
    df.to_csv('s3://bibhash-airflow-bucket/Imdb_reviews.csv')


