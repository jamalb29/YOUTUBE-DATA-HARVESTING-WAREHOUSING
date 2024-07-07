from googleapiclient.discovery import build
import pandas as pd
import mysql.connector
import pymysql
import pandas as pd
import sqlalchemy
import streamlit as st
from sqlalchemy import create_engine
from isodate import parse_duration

#API Connect
def Api_connect():
   api_key='AIzaSyDLhc-8TuTOs9sFpw9s1mci06pMhOW_ruU'
   api_service_name = "youtube"
   api_version = "v3"
   youtube = build(api_service_name, api_version, developerKey=api_key)
   
   return youtube 
youtube =Api_connect() 


# function to get the Channel Details 
def get_channel_details(c_id):
 request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c_id
    )
 response = request.execute()  
 
 data={
     "channel_name" : response['items'][0]['snippet']['title'],
     "published_at" : response['items'][0]['snippet']['publishedAt'],
     "p_id"         : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
     "channel_views": response['items'][0]['statistics']['viewCount'],
     "sub_count"    : response['items'][0]['statistics']['subscriberCount'],
     "video_count"  : response['items'][0]['statistics']['videoCount']
       }
 return data

# Function to get the Playlist information of the channel
def playlist_details(c_id):
    playlist_info=[]
    nextPageToken=None
    while True:
        request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=c_id,
                    maxResults=50,
                    pageToken=nextPageToken
                )
        response= request.execute()

        for i in response["items"]:
            data=dict(
                playlist_id=i['id'],
                playlist_name=i['snippet']['title'],
                channel_ID=i['snippet']['channelId'],
                channel_name=i['snippet']['channelTitle'],
                videoscount=i['contentDetails']['itemCount'])
            playlist_info.append(data)
            nextPageToken=response.get('nextPageToken')
        if nextPageToken is None:
            break
    return(playlist_info)        
            
# Function to get the video Ids  of the channel
def get_video_id(c_id):
    video_ids =[]
   
    res = youtube.channels().list(part="snippet,contentDetails,statistics",
           id=c_id ).execute()  
    playlist_Id= res['items'][0]['contentDetails']['relatedPlaylists']['uploads'] 
    next_page_token = None
       
    while True:
        
       res = youtube.playlistItems().list(part="snippet,contentDetails",
               maxResults=50,
               playlistId= playlist_Id,
               pageToken=next_page_token).execute()

       for i in range(len(res['items'])):
               video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
       next_page_token =res.get('nextPageToken') 
       
       if next_page_token is None :
           break
        
      
    return video_ids
# Function to get the video information of the channel

def dur(duration_str):
    duration_seconds=parse_duration(duration_str).total_seconds()
    return duration_seconds

def get_video_info(video_ids):
    video_data =[]
    for video_id in video_ids :
        request = youtube.videos().list(
             part="snippet,contentDetails,statistics",
             id=video_id
        )
        response = request.execute()
            
        for item in response['items']:
            data =dict( Channel_Name = item["snippet"]["channelTitle"],
                      Channel_Id =item["snippet"]["channelId"],
                      Video_Id =item['id'],
                       Title =item['snippet']['title'],
                       Description =item['snippet'].get('description'),
                       Published_Date =item['snippet']['publishedAt'],
                       Duration =parse_duration(item['contentDetails']['duration']).total_seconds(),
                       Views =item['statistics'].get('viewCount'),
                       Likes=item['statistics'].get('likeCount'),
                       Dislikes=item['statistics'].get('dislikeCount'),
                       Comment=item['statistics'].get('commentCount'),
                       Favorite_COunt=item['statistics']['favoriteCount'],
                       Definition=item['contentDetails']['caption']
                       
                      )
            video_data.append(data)
            
    return video_data 


# Function to get the Comment information of the channel
def get_comment_info(video_ids):
    Comment_data=[]
    
    for video_id in video_ids:
        try:
            request=youtube.commentThreads().list(
             part="snippet",
            videoId=video_id,
            maxResults=50
            
            )
            
            response=request.execute()
            
            for item in response['items']:
                data =dict ( Comment_Id = item['snippet']['topLevelComment']['id'],
                             Video_Id =item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text =item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published= item['snippet']['topLevelComment']['snippet']['publishedAt'])                          
                                                     
                            
                Comment_data.append(data)
        except:
               pass
    return Comment_data
def main(c_id):
    c = get_channel_details(c_id)
    pl =playlist_details(c_id)
    v_id = get_video_id(c_id)
    v=  get_video_info(v_id)
    cm = get_comment_info(v_id)
    
    data = { "channel details" : c ,
            "playlist" : pl ,
           "videoids" : v_id ,
            "videodetails" : v ,
            "detals" : cm
          
           }
    return data

def dur(duration_str):
        duration_seconds=parse_duration(duration_str).total_seconds()
        return duration_seconds

st.title('YouTube DataHarvesting')
c_id=st.text_input('Enter the Channel ID')
option = st.selectbox('my project 3 stage ',("scraping","Migrate to Mysql","query"))
if option=='scraping':

    if c_id and st.button ('Scrape'):
        overall_details=main(c_id)
        st.write(overall_details)

if option=="Migrate to Mysql":

# MYSQL CONNECTION AND TABLE CREATION

    db_data = 'mysql+mysqldb://' + 'root' + ':' + 'jamal29.mech' + '@' + '127.0.0.1' + ':3306/' \
        + 'mydatabase' + '?charset=utf8mb4'
    engine = create_engine(db_data)

    connection = pymysql.connect(host='localhost',
                            user='root',
                            password='jamal29.mech',
                            db='mydatabase') 
    cursor=connection.cursor()


    #Transform Corresponding datain to Dateframe

    df_channel=pd.DataFrame(get_channel_details(c_id), index=[0])
    df_playlist=pd.DataFrame( playlist_details(c_id))
    df_videos=pd.DataFrame(get_video_info(video_ids=get_video_id(c_id)))
    df_comments=pd.DataFrame(get_comment_info(video_ids=get_video_id(c_id)))


    #TO load the Details in My SQl
    df_channel.to_sql('channel_details', engine, if_exists='append', index=False)  

    df_playlist.to_sql('playlist',engine, if_exists='append', index=False,
                dtype={'playlist_id': sqlalchemy.types.VARCHAR(length=50),
                        'playlist_name': sqlalchemy.types.VARCHAR(length=100),
                         'channel_ID': sqlalchemy.types.VARCHAR(length=50),
                        'channel_name': sqlalchemy.types.VARCHAR(length=100),
                        'videoscount': sqlalchemy.types.BIGINT,})

    df_videos.to_sql('video_details', engine, if_exists='append', index=False,   
                dtype={'Channel_Name': sqlalchemy.types.VARCHAR(length=225),
                                    'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                                    'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                    'Title': sqlalchemy.types.TEXT,
                                    'Description': sqlalchemy.types.TEXT,
                                    'Published_Date': sqlalchemy.types.String(length=50),
                                    'Duration':sqlalchemy.types.VARCHAR(length=1024),
                                    'Views': sqlalchemy.types.INT,
                                    'Likes': sqlalchemy.types.INT,
                                    'DisLikes': sqlalchemy.types.INT,
                                    'Comment': sqlalchemy.types.VARCHAR(length=225),
                                    'Favorite_COunt': sqlalchemy.types.INT,
                                    'Definition':sqlalchemy.types.VARCHAR(length=225),
                                   })

    df_comments.to_sql('df_comments', engine, if_exists='append', index=False,
                dtype={'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                        'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                        'Comment_Text': sqlalchemy.types.TEXT,
                        'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                        'Comment_Published_date': sqlalchemy.types.String(length=50), })
    
    st.success('Uploaded in MYSQL') 


# QUERY ZONE
if option == "query":

    selected_question = st.selectbox('Select your Question]',
                                  ('1. What are the names of all the videos and their corresponding channels?',
                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                  key='collection_question')

    # Create a connection to SQL
    db_data = 'mysql+mysqldb://' + 'root' + ':' + 'jamal29.mech' + '@' + '127.0.0.1' + ':3306/' \
        + 'mydatabase' + '?charset=utf8mb4'
    engine = create_engine(db_data)

    connection = pymysql.connect(host='localhost',
                            user='root',
                            password='jamal29.mech',
                            db='mydatabase') 
    cursor=connection.cursor()
  
      # Q1
    if selected_question == '1. What are the names of all the videos and their corresponding channels?':        
        query1='''select Title as videos,Channel_Name as channelname from video_details'''
        cursor.execute(query1)
        connection.commit()
        t1=cursor.fetchall()
        dfq1=pd.DataFrame(t1,columns=["videotitle","channelname"])
        dfq1
   

     # Q2
    elif selected_question == '2. Which channels have the most number of videos, and how many videos do they have?':

        query2='''select channel_name as channelname,video_count as no_of_videos from channel_details order by video_count desc'''
        cursor.execute(query2)
        connection.commit()
        t2=cursor.fetchall()
        dfq2=pd.DataFrame(t2,columns=["channelname","No_of_Videos"])
        dfq2
                
     # Q3   
    elif selected_question == '3. What are the top 10 most viewed videos and their respective channels?':
    
      query3='''select Channel_Name as channel_name,Title as video_title ,Views as views from video_details
                  where Views is not  null order by Views desc limit 10'''
      cursor.execute(query3)
      connection.commit()
      t3=cursor.fetchall()
      dfq3=pd.DataFrame(t3,columns=["channel_name","video_title","Views"])
      dfq3

    # Q4
    elif selected_question == '4. How many comments were made on each video, and what are their corresponding video names?':
        
        query4='''select Comment as no_of_comments,Title as video_title from video_details 
                where Comment is not  null '''
        cursor.execute(query4)
        connection.commit()
        t4=cursor.fetchall()
        dfq4=pd.DataFrame(t4,columns=["no_of_comments","video_title"])
        dfq4

    # Q5
    elif selected_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':

        query5='''select Title as video_title,Channel_Name as channel_name,Likes as likescount from video_details
            where Likes is not  null order by Likes desc  '''
        cursor.execute(query5)
        connection.commit()
        t5=cursor.fetchall()
        dfq5=pd.DataFrame(t5,columns=[" video_title","channel_name","likescount"])
        dfq5

    # Q6
    elif selected_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':

        query6='''select Title as video_title,Likes as likescount from video_details  '''
        cursor.execute(query6)
        connection.commit()
        t6=cursor.fetchall()
        dfq6=pd.DataFrame(t6,columns=[" video_title","likescount"])
        dfq6

    # Q7
    elif selected_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
      
        query7='''select Channel_Name as channel_name,channel_views as viewscount from channel_details  ORDER BY channel_views DESC  '''
        cursor.execute(query7)
        connection.commit()
        t7=cursor.fetchall()
        dfq7=pd.DataFrame(t7,columns=[" channel_name","viewscount"])
        dfq7
       
    # Q8
    elif selected_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        query8='''select Title as video_title,Published_Date as video_published ,Channel_Name as channel_name from video_details
            where extract(year from Published_Date)= 2023'''
        cursor.execute(query8)
        connection.commit()
        t8=cursor.fetchall()
        dfq8=pd.DataFrame(t8,columns=[" video_title","video_published","channel_name"])
        dfq8

     # Q9
    elif selected_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
      
        query9='''select Channel_Name as channel_name ,AVG(Duration) as averageduration from video_details
           group by Channel_Name'''
        cursor.execute(query9)
        connection.commit()
        t9=cursor.fetchall()
        dfq9=pd.DataFrame(t9,columns=["channel_name","averageduration"])
        dfq9
      
    # Q10
    elif selected_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10='''select Title as video_title ,Channel_Name as channel_name, Comment as comment from video_details
          where Comment is not null order by Comment desc '''
        cursor.execute(query10)
        connection.commit()
        t10=cursor.fetchall()
        dfq10=pd.DataFrame(t10,columns=["video_title","channel_name","comment"])
        dfq10

        # MySQL DB connection close
    connection.close()
