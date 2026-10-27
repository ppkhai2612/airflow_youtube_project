import os
import json

from dotenv import load_dotenv
from googleapiclient.discovery import build


load_dotenv('dags/.env')
api_key = os.environ.get("YOUTUBE_API_KEY")

youtube = build("youtube", "v3", developerKey=api_key)
request = youtube.videoCategories().list(
    part="snippet",
    regionCode="VN"
)
response = request.execute()

categories = {item['id']:item['snippet']['title'] for item in response['items']}

# save categories to json file
with open('dags/categories.json', 'w') as f:
    json.dump(categories, f, indent=2)