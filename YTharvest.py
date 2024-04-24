import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
import streamlit as st
import re

# Set up YouTube Data API access
api_key = "YOUR_API_KEY"
youtube = build('youtube', 'v3', developerKey=api_key)


#Extract Channel data using channel_id copied from a specific YouTube channel
def extract_channel_data(channel_id):
    channel_response = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    ).execute()
    channel_snippet = channel_response['items'][0]['snippet']
    channel_statistics = channel_response['items'][0]['statistics']
    channel_contentDetails = channel_response['items'][0]['contentDetails']
    return {
        'Channel_name': channel_snippet['title'],
        'Channel_ID': channel_id,
        'Subscription_count': channel_statistics.get('subscriberCount', 0),
        'Channel_views': channel_statistics.get('viewCount', 0),
        'Channel_description': channel_snippet.get('description', ''),
        'Playlist_ID': channel_contentDetails.get('relatedPlaylists', {}).get('uploads', '')
    }


#Extract video_ids of the YouTube channel using the playlist_id
def extract_video_ids(playlist_id):
    video_ids = []
    next_page_token = None
    while True:
        playlist_response = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in playlist_response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids


#Extract details of all the videos in the YouTube channel using the video_ids
def extract_video_data(video_id):
    video_response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    ).execute()
    video_snippet = video_response['items'][0]['snippet']
    video_statistics = video_response['items'][0]['statistics']
    video_content_details = video_response['items'][0]['contentDetails']
    
    time_string = video_content_details['duration']
    minutes = convert_to_minutes(time_string)
    return {
        'Channel_ID': channel_id,
        'Video_ID': video_id,
        'Video_name': video_snippet['title'],
        'Video_description': video_snippet.get('description', ''),
        'Tags': video_snippet.get('tags', []),
        'Published_at': video_snippet['publishedAt'],
        'View_count': video_statistics.get('viewCount', 0),
        'Like_count': video_statistics.get('likeCount', 0),
        'Dislike_count': video_statistics.get('dislikeCount', 0),
        'Favorite_count': video_statistics.get('favoriteCount', 0),
        'Comment_count': video_statistics.get('commentCount', 0),
        'Duration': minutes,
        'Thumbnail': video_snippet['thumbnails']['default']['url'],
        'Caption_status': video_content_details['caption'],
        'Comments': []
    }

#Converting the Duration of the video in minutes for easy processing of the data in database
def convert_to_minutes(time_string):
    hour_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_min_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M', time_string)
    min_sec_match = re.match(r'PT(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_sec_match = re.match(r'PT(?P<hours>\d+)H(?P<seconds>\d+)S', time_string)
    hour_only_match = re.match(r'PT(?P<hours>\d+)H', time_string)
    minute_match = re.match(r'PT(?P<minutes>\d+)M', time_string)
    sec_match = re.match(r'PT(?P<seconds>\d+)S', time_string)
    
    if hour_match:
        hours = int(hour_match.group('hours'))
        minutes = int(hour_match.group('minutes'))
        seconds = int(hour_match.group('seconds'))
        return hours * 60 + minutes + seconds / 60
    elif hour_min_match:
        hours = int(hour_min_match.group('hours'))
        minutes = int(hour_match.group('minutes'))
        return hours * 60 + minutes
    elif min_sec_match:
        minutes = int(min_sec_match.group('minutes'))
        seconds = int(min_sec_match.group('seconds'))
        return minutes + seconds / 60
    elif hour_sec_match:
        hours = int(hour_sec_match.group('hours'))
        seconds = int(hour_sec_match.group('seconds'))
        return hours * 60 + seconds / 60
    elif hour_only_match:
        hours = int(hour_only_match.group('hours'))
        return hours * 60
    elif minute_match:
        minutes = int(minute_match.group('minutes'))
        return minutes
    elif sec_match:
        seconds = int(sec_match.group('seconds'))
        return seconds / 60
    else:
      raise ValueError('Invalid time string: {}'.format(time_string))


#Extract the first 100 comments of each video in the YouTube channel using the video_ids
def extract_comments(video_id):
    comments = []
    next_page_token = None
    while True:
        comment_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token
        ).execute()
        for item in comment_response['items']:
            comment_snippet = {'Channel_ID': channel_id,
                               'Video_ID': video_id,
                               'Comment_text':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                               'Comment_ID':item['snippet']['topLevelComment']['id'],
                               'Author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                               'Published_at':item['snippet']['topLevelComment']['snippet']['publishedAt']
                               }
            comments.append(comment_snippet)
        next_page_token = comment_response.get('nextPageToken')
        if not next_page_token:
            break
    return comments


#Extract all the data of the YouTube channel(Channel_data, Video_data, Comment_data) by calling the functions
def extract_data(channel_id):
    channel_data = extract_channel_data(channel_id)
    video_ids = extract_video_ids(channel_data['Playlist_ID'])
    video_data = []
    comment_data = []
    for video_id in video_ids:
        video_details = extract_video_data(video_id)
        video_details['Comments'] = extract_comments(video_id)
        video_data.append(video_details)
        comment_data.append(video_details['Comments'])
        video_df = pd.DataFrame(video_data)
        comment_df = pd.DataFrame(comment_data)
    return channel_data, video_df, comment_df

#-------------------------------------------------------------------------------------------------------------------------------------

#Inserting the extracted data into the DATABASE using SQL

mydb = mysql.connector.connect(host = "localhost", user = "root", password = "")
print(mydb)
mycursor = mydb.cursor(buffered = True)

#mycursor.execute('create database youtube_test')
mycursor.execute('use youtube_test')
#mycursor.execute('create table channels (Channel_Name VARCHAR(100), Channel_ID VARCHAR(50), Subscribers INT(50), Channel_Views INT(50), Channel_Description VARCHAR(10000))')
#mycursor.execute('create table videos (Channel_ID VARCHAR(100), Video_ID VARCHAR(50), Video_Name VARCHAR(50), Video_Description VARCHAR(10000), Tags VARCHAR(500), Published_At DATETIME, Views INT(50), Likes INT(50), Dislikes INT(50), Favorites INT(50), Comment_count INT(100), Duration VARCHAR(50), Thumbnail VARCHAR(50), Caption_Status VARCHAR(50))')
#mycursor.execute('create table comments (Channel_ID VARCHAR(100), Video_ID VARCHAR(50), Comment_text VARCHAR(1000), Comment_ID VARCHAR(50), Author VARCHAR(100), Published_At DATETIME)')


#Insert the channel data, video data and comment data into SQL
def insert_data(channel_data, video_df):
    # Insert channel data
    mycursor.execute("INSERT INTO channels (Channel_name, Channel_ID, Subscribers, Channel_views, Channel_description) VALUES (%s, %s, %s, %s, %s)",
                   (channel_data['Channel_name'], channel_data['Channel_ID'], channel_data['Subscription_count'], channel_data['Channel_views'], channel_data['Channel_description']))
    mydb.commit()

    # Insert video data
    for _, row in video_df.iterrows():
        mycursor.execute("INSERT INTO videos (Channel_ID, Video_ID, Video_Name, Video_Description, Tags, Published_At, Views, Likes, Dislikes, Favorites, Comment_count, Duration, Thumbnail, Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (channel_data['Channel_ID'], row['Video_ID'], row['Video_name'], row['Video_description'], ','.join(row['Tags']), row['Published_at'], row['View_count'], row['Like_count'], row['Dislike_count'], row['Favorite_count'], row['Comment_count'], row['Duration'], row['Thumbnail'], row['Caption_status']))
        
        mydb.commit()
    
    for _, row in video_df.iterrows():
        for comment in row['Comments']:
          mycursor.execute("INSERT INTO comments (Channel_ID, Video_ID, Comment_text, Comment_ID, Author, Published_at) VALUES (%s, %s, %s, %s, %s, %s)",
                          (channel_data['Channel_ID'], row['Video_ID'], comment['Comment_text'], comment['Comment_ID'], comment['Author_name'], comment['Published_at']))
          mydb.commit()


#Call the function to insert the data in to SQL
#insert_data(channel_data, video_df)



#Creating a streamlit application with the query of the questions given for the data stored in the local database

st.title("YouTube Channel Data Extraction")
channel_id = st.text_input("Enter YouTube Channel ID:")

def extract_insert_data_st():
    if st.button("Extract Data and Store in Database"):
            channel_data, video_df, comment_df = extract_data(channel_id)
            insert_data(channel_data, video_df)
            st.success("Data extraction and storage successful!")

extract_insert_data_st()

query_options = [
    "What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
selected_query = st.selectbox("Select Question:", query_options)

if st.button("Execute"):
    mydb = mysql.connector.connect(host = "localhost", user = "root", password = "", database = "youtube_test")
    if selected_query == query_options[0]:
        query_result = pd.read_sql_query("SELECT Video_Name, Channel_Name FROM videos INNER JOIN channels ON videos.Channel_ID = channels.Channel_ID", mydb)
    elif selected_query == query_options[1]:
        query_result = pd.read_sql_query("SELECT Channel_Name, COUNT(Video_ID) AS Num_Videos FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID GROUP BY Channel_Name ORDER BY Num_Videos DESC LIMIT 1", mydb)
    elif selected_query == query_options[2]:
        query_result = pd.read_sql_query("SELECT Video_Name, Channel_Name FROM videos INNER JOIN channels ON videos.Channel_ID = channels.Channel_ID ORDER BY Views DESC LIMIT 10;", mydb)
    elif selected_query == query_options[3]:
        query_result = pd.read_sql_query("SELECT Video_Name, COUNT(Comment_ID) AS Number_of_Comments FROM videos INNER JOIN comments ON videos.Video_ID = comments.Video_ID GROUP BY Video_Name", mydb)
    elif selected_query == query_options[4]:
        query_result = pd.read_sql_query("SELECT Video_Name, Channel_Name FROM videos INNER JOIN channels ON videos.channel_ID = channels.channel_ID ORDER BY Likes DESC LIMIT 1", mydb)
    elif selected_query == query_options[5]:
        query_result = pd.read_sql_query("SELECT Video_Name, SUM(Likes) AS Total_Likes, SUM(Dislikes) AS Total_Dislikes FROM videos GROUP BY Video_Name", mydb)
    elif selected_query == query_options[6]:
        query_result = pd.read_sql_query("SELECT Channel_Name, SUM(Views) AS Total_Views FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID GROUP BY Channel_Name", mydb)
        
    elif selected_query == query_options[7]:
        query_result = pd.read_sql_query("SELECT Channel_Name FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID WHERE SUBSTRING(videos.Published_At, 1, 4) = '2022' GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[8]:
        query_result = pd.read_sql_query("SELECT Channel_Name, AVG(Duration) AS Average_Duration FROM channels INNER JOIN videos ON videos.Channel_ID = channels.Channel_ID GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[9]:
        query_result = pd.read_sql_query("SELECT Video_Name, Channel_Name FROM videos INNER JOIN channels ON videos.Channel_ID = channels.Channel_ID ORDER BY Comment_count DESC LIMIT 1", mydb)
    mydb.close()

    st.dataframe(query_result)






