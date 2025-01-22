import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from datetime import datetime

import boto3

def lambda_handler(event, context):

    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlist_link = "https://open.spotify.com/playlist/2a4YYkZa1c4ah4KFSGw5u5"
    playlist_URI = playlist_link.split('/')[-1]

    data = sp.playlist_tracks(playlist_URI)

    client = boto3.client('s3')

    file_name = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket='spotify-etl-datapileline-vjaggavarapu',
        Body=json.dumps(data),
        Key='raw_data/to_processed/'+ file_name
    )

