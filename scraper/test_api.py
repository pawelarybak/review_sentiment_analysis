import requests
import pandas as pd

API_URL = 'http://localhost:8080/analyze'
test_file = 'test.csv'

def get_response(review: str):
    response = requests.get(
        API_URL,
        payload=review,
    )
    return float(response.text)

def main():
    reviews = pd.read_csv(test_file)
    values = (get_response(rev) for rev in reviews['text'])
    results = pd.DataFrame(
        zip(values, reviews['rating']),
        columns=['prediction', 'label'],
    )
    results.to_csv('test_results.csv')

if __name__ == "__main__":
    main()