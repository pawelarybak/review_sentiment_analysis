import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = 'https://www.filmweb.pl'
MOVIE_SEARCH_URL = '{}/films/search'.format(BASE_URL)


def get_links(max_movies=None):
    page = 1
    fetched = 0
    while True:
        response = requests.get(
            MOVIE_SEARCH_URL,
            {'page': page},
        )
        if response.status_code != 200:
            raise Exception

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.select('.filmPreview__link')
        if not links:
            return

        for link in links:
            fetched += 1
            if max_movies and fetched > max_movies:
                return

            yield link.get('href')
        page += 1


def get_reviews_links(movie_url):
    response = requests.get('{}/reviews'.format(movie_url))
    if response.status_code != 200:
        raise Exception

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.select('.allReviews .text a')

    return (link.get('href') for link in links)


def get_review(review_url):
    response = requests.get(review_url)
    if response.status_code != 200:
        raise Exception

    soup = BeautifulSoup(response.text, 'html.parser')
    text_container = soup.select('.reviewPage .text')[0]

    if text_container.script:
        text_container.script.decompose()

    review_text = text_container.text

    rating_container = soup.select('span[itemprop=ratingValue]')

    if not rating_container:
        return None, review_text

    rating = rating_container[0].text
    return rating, review_text


def get_reviews_for_movie(url):
    reviews_links = get_reviews_links(url)
    return [get_review(BASE_URL + review_url) for review_url in reviews_links]


if __name__ == '__main__':
    links = get_links()
    reviews_data = pd.DataFrame()
    for idx, link in enumerate(links, start=1):
        reviews = get_reviews_for_movie(BASE_URL + link)
        reviews_data = pd.concat([
            reviews_data,
            pd.DataFrame(reviews, columns=['rating', 'text']),
        ])
        print('Fetched {} reviews from {} movies'.format(len(reviews_data), idx))
    reviews_data.to_csv('reviews.csv')
