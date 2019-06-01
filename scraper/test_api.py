import requests
import pandas as pd

API_URL = 'http://localhost:8000/analyze'
test_file = 'test.csv'

def get_response(review: str):
    response = requests.get(
        API_URL,
        data=review.encode('utf-8'),
    )
    return float(response.text)

def get_data(reviews):
    for idx, review in enumerate(reviews, start=1):
        print('{}/{}'.format(idx, len(reviews)))
        yield get_response(review)
    return

def main():
    reviews = pd.read_csv(test_file)
    results = pd.DataFrame(
        zip(get_data(reviews['text']), reviews['rating']),
        columns=['prediction', 'label'],
    )
    results.to_csv('test_results.csv')

if __name__ == "__main__":
    main()