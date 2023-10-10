import requests

response = requests.get('https://www.reddit.com/api/v1/me', headers={'User-Agent': 'test'})

print(response.json())
