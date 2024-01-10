import streamlit as st
import pymongo
from googleapiclient.discovery import build

def app():
    #API key connection
    def api_connect():
        api_id="AIzaSyDTWroAsY92ojVHybg_Zc0rK6R4pXNZSyk"
        api_service_name="youtube"
        api_version="v3"
        yt=build(api_service_name,api_version,developerKey=api_id)
        return yt
    yt=api_connect()

    #get Channel information
    def get_channel_info(channel_id):
        request=yt.channels().list(
            part="snippet,ContentDetails,statistics", id=channel_id
        )
        response=request.execute()
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['title'],
                    Channel_Id=i['id'],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Description=i['snippet']['description'],
                    Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
        return data

    #Get Video ids
    #Using Playlist_Id we can retrieve the video id
    def get_videos_ids(channel_id):
        video_ids=[]
        res=yt.channels().list(id=channel_id,
                            part='contentDetails').execute()
        Playlist_id=res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token=None
        while True:
            res1=yt.playlistItems().list(
                                    part='snippet',
                                    playlistId=Playlist_id,
                                    maxResults=50,
                                    pageToken=next_page_token).execute()
            for i in range(len(res1['items'])):
                video_ids.append(res1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=res1.get('nextPageToken')   
            if next_page_token is None:
                break
        return video_ids


    #Video details
    def get_video_info(video_ids):
        video_data=[]
        for vid_id in video_ids:
            request=yt.videos().list(
                part="snippet,ContentDetails,statistics",
                id=vid_id
            )
            response=request.execute()
            for item in response["items"]:
                data=dict(Channel_Name=item["snippet"]["channelTitle"],
                        Channel_Id=item["snippet"]["channelId"],
                        Video_Id=item['id'],
                        Title=item["snippet"]["title"],
                        Tags=item['snippet'].get("tags"),
                        Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                        Description=item['snippet'].get("description"),
                        Published_Date=item["snippet"]["publishedAt"],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics'].get("viewCount"),
                        Likes=item['statistics'].get("likeCount"),
                        Comments=item['statistics'].get('commentCount'),
                        Favourite_Count=item['statistics'].get('favoriteCount'),
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption'])
                video_data.append(data)
        return video_data


    #get Comment info
    def get_comment_info(video_ids):
        Comment_data=[]
        try:
            for video_id in video_ids:
                request=yt.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()
                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    Comment_data.append(data)
        except:
            pass
        return Comment_data

    #Get Playlist Info
    def get_playlist_info(channel_id):
        next_page_token=None
        All_data=[]
        while True:
            request=yt.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
                All_data.append(data)
            next_page_token=response.get('nextPageToken')    
            if next_page_token is None:
                break
        return All_data    

    #Upload to Mongo
    client=pymongo.MongoClient("mongodb://localhost:27017")
    db=client['Youtube_data']

    def channel_details(channel_id):
        ch_details=get_channel_info(channel_id)
        pl_details=get_playlist_info(channel_id)
        vi_ids=get_videos_ids(channel_id)
        vi_details=get_video_info(vi_ids)
        com_details=get_comment_info(vi_ids)
        coll1=db['channel_details']
        
        coll1.insert_one({"channel_information":ch_details,
                        "playlist_information":pl_details,
                        "video_information":vi_details,
                        "comment_information":com_details
                        })
        return "Uploaded Successfully!"

    def channel_details(channel_id):
        ch_details=get_channel_info(channel_id)
        pl_details=get_playlist_info(channel_id)
        vi_ids=get_videos_ids(channel_id)
        vi_details=get_video_info(vi_ids)
        com_details=get_comment_info(vi_ids)
        coll1=db['channel_details']
        
        coll1.insert_one({"channel_information":ch_details,
                        "playlist_information":pl_details,
                        "video_information":vi_details,
                        "comment_information":com_details
                        })
        return "Uploaded Successfully!"
    client=pymongo.MongoClient("mongodb://localhost:27017")
    def display_channels_table():
        ch_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data['channel_information'])
        df=st.dataframe(ch_list)
        return df

    #Upload to Mongo
    client=pymongo.MongoClient("mongodb://localhost:27017")
    db=client['Youtube_data']

    channel_id=st.text_input("Enter the Channel Id")
    if st.button("Collect and Store data"):
        ch_ids=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for ch_data in coll1.find({},{'_id':0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel_id in ch_ids:
            st.success("Channel Details of the given Channel Id already exits")
        else:
            insert=channel_details(channel_id)    
            st.success(insert)
    display_channels_table()       