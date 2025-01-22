import json
import boto3
import pandas as pd

from io import StringIO

from datetime import datetime

def songs(data):
    songs_list = []

    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                    }
        songs_list.append(song_element)

    return songs_list
    
def artists(data):
    artists_list = []

    for track in data['items']:
        for key, value in track.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_data = {"artist_id": artist['id'],"artist_name":artist['name'],'artist_url':artist['href']}
                    artists_list.append(artist_data)
    return artists_list

def albums(data):
    albums_list = []

    for track in data['items']:
        
        album_id = track['track']['album']['id']
        album_name = track['track']['album']['name']
        album_release_date = track['track']['album']['release_date']
        album_total_tracks = track['track']['album']['total_tracks']
        album_url = track['track']['album']['external_urls']['spotify']

        album = {"album_id":album_id, "album_name":album_name, "release_date":album_release_date, "total_tracks":album_total_tracks,"album_url":album_url}

        albums_list.append(album)

    return albums_list

def lambda_handler(event, context):

    client = boto3.client('s3')
    
    Bucket = 'spotify-etl-datapileline-vjaggavarapu'
    Key = 'raw_data/to_processed/'

    spotify_data = []
    spotify_keys = []

    for file in client.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']

        if file_key.endswith('.json'):
            response = client.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            json_content = json.loads(content.read())

            spotify_data.append(json_content)
            spotify_keys.append(file_key)

    for data in spotify_data:
        albums_list = albums(data)
        artists_list = artists(data)
        songs_list = songs(data)

        print(albums_list)

        albums_df = pd.DataFrame.from_dict(albums_list)
        albums_df.drop_duplicates(subset=['album_id'])

        artists_df = pd.DataFrame.from_dict(artists_list)
        artists_df.drop_duplicates(subset=['artist_id'])

        songs_df = pd.DataFrame.from_dict(songs_list)

        albums_df['release_date'] = pd.to_datetime(albums_df['release_date'],errors='coerce')
    
        songs_df['song_added'] = pd.to_datetime(songs_df['song_added'],errors='coerce')
         

        song_key = "transformed_data/songs_data/song_transformed_" + str(datetime.now()) +  ".csv"

        song_buffer = StringIO()
        songs_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        
        artist_key = "transformed_data/artists_data/artist_transformed_" + str(datetime.now()) +  ".csv"

        artist_buffer = StringIO()
        artists_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

        album_key = "transformed_data/albums_data/album_transformed_" + str(datetime.now()) +  ".csv"

        album_buffer = StringIO()
        albums_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        s3_resource = boto3.resource('s3')

    for key in spotify_keys:
        copy_source = {
                'Bucket': Bucket,
                'Key': key
            }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()

        
    
