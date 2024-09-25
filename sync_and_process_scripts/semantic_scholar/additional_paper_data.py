import requests
from config import api_key
import json

def get_additional_citation_data(df):
    print("Hello world",df)


# def main():
    
#     r = requests.post(
#     'https://api.semanticscholar.org/graph/v1/paper/batch',
#     params={'fields': 'referenceCount,citationCount'},
#     json={"ids": ["649def34f8be52c8b66281af98ae884c09aef38b", "ARXIV:2106.15928"]}
#     )

#     print(json.dumps(r.json(), indent=2))
    

# if __name__ == "__main__":
#     main()