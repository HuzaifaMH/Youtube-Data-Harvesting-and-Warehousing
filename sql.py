import streamlit as st
import psycopg2
import pandas as pd
import pymongo

def app():
    #Table Creation for channels,playlists,videos and comments
    def channels_table():
        mydb=psycopg2.connect(host="localhost",
                            user='postgres',
                            password='root',
                            database='youtube_data',
                            port='5432')
        cursor=mydb.cursor()

        drop_query='''drop table if exists channels'''
        cursor.execute(drop_query)
        mydb.commit()

        try:
            create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(80) primary key,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total_Videos int,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(80))'''
            cursor.execute(create_query)
            mydb.commit()
        except:
            print("Channel Table already created!")    

        ch_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']

        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data['channel_information'])
        df=pd.DataFrame(ch_list)

        for index,row in df.iterrows():
            insert_query='''insert into channels (Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos
                                                ,Channel_Description
                                                ,Playlist_Id)
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
            
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print('Channels values are already inserted')        
                

    def playlists_table():
        mydb=psycopg2.connect(host="localhost",
                            user='postgres',
                            password='root',
                            database='youtube_data',
                            port='5432')
        cursor=mydb.cursor()

        drop_query='''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists playlists( Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int)'''
        cursor.execute(create_query)
        mydb.commit()   
        
        pl_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']

        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(pl_data['playlist_information'])):
                pl_list.append(pl_data['playlist_information'][i])
        df1=pd.DataFrame(pl_list)
        
        for index,row in df1.iterrows():
            insert_query='''insert into playlists (Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                values(%s,%s,%s,%s,%s,%s)'''
            values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])
            cursor.execute(insert_query,values)
            mydb.commit()   

            

    #Table creation for videos
    def videos_table():
            mydb=psycopg2.connect(host="localhost",
                            user='postgres',
                            password='root',
                            database='youtube_data',
                            port='5432')
            cursor=mydb.cursor()

            drop_query='''drop table if exists videos'''
            cursor.execute(drop_query)
            mydb.commit()

            create_query='''create table if not exists videos(Channel_Name varchar(100),
                            Channel_Id varchar(100),
                            Video_Id varchar(30) primary key,
                            Title varchar(150),
                            Tags text,
                            Thumbnail varchar(200),
                            Description text,
                            Published_Date timestamp,
                            Duration interval,
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Favourite_Count int,
                            Definition varchar(10),
                            Caption_Status varchar(50))'''
            cursor.execute(create_query)
            mydb.commit()   

            vi_list=[]
            db=client['Youtube_data']
            coll1=db['channel_details']

            for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                    for i in range(len(vi_data['video_information'])):
                            vi_list.append(vi_data['video_information'][i])
                    df2=pd.DataFrame(vi_list)


            for index,row in df2.iterrows():
                    insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favourite_Count,
                                                    Definition,
                                                    Caption_Status)
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    values=(row['Channel_Name'],
                            row['Channel_Id'],
                            row['Video_Id'],
                            row['Title'],
                            row['Tags'],
                            row['Thumbnail'],
                            row['Description'],
                            row['Published_Date'],
                            row['Duration'],
                            row['Views'],
                            row['Likes'],
                            row['Comments'],
                            row['Favourite_Count'],
                            row['Definition'],
                            row['Caption_Status'])
                    cursor.execute(insert_query,values)
                    mydb.commit() 
        


    #Table Creation for comments
    def comments_table():
            mydb=psycopg2.connect(host="localhost",
                                    user='postgres',
                                    password='root',
                                    database='youtube_data',
                                    port='5432')
            cursor=mydb.cursor()

            drop_query='''drop table if exists comments'''
            cursor.execute(drop_query)
            mydb.commit()

            create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                            Video_Id varchar(50),
                            Comment_Text text,
                            Comment_Author varchar(150),
                            Comment_Published timestamp)'''
            cursor.execute(create_query)
            mydb.commit()   

            com_list=[]
            db=client['Youtube_data']
            coll1=db['channel_details']

            for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                    for i in range(len(com_data['comment_information'])):
                            com_list.append(com_data['comment_information'][i])
            df3=pd.DataFrame(com_list)

            for index,row in df3.iterrows():
                    insert_query='''insert into comments (Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published)
                                                    values(%s,%s,%s,%s,%s)'''
                    values=(row['Comment_Id'],
                            row['Video_Id'],
                            row['Comment_Text'],
                            row['Comment_Author'],
                            row['Comment_Published'])
                    cursor.execute(insert_query,values)
                    mydb.commit()

    def tables():
        channels_table()
        playlists_table()
        videos_table()
        comments_table()
        return "Tables created successfully"

    client=pymongo.MongoClient("mongodb://localhost:27017")
    def show_channels_table():
        ch_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data['channel_information'])
        df=st.dataframe(ch_list)
        return df

    def show_playlists_table():
        pl_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(pl_data['playlist_information'])):
                pl_list.append(pl_data['playlist_information'][i])
        df1=st.dataframe(pl_list)
        return df1

    def show_videos_table():
        vi_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data['video_information'])):
                vi_list.append(vi_data['video_information'][i])
        df2=st.dataframe(vi_list)
        return df2

    def show_comments_table():
        com_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data['comment_information'])):
                        com_list.append(com_data['comment_information'][i])
        df3=st.dataframe(com_list)
        return df3

    if st.button("Migrate to SQL"):
        Table=tables()
        st.success(Table)
    show_tables=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
    if show_tables=="CHANNELS":  
        show_channels_table()      
    elif show_tables=="PLAYLISTS":  
        show_playlists_table()      
    elif show_tables=="VIDEOS":  
        show_videos_table()      
    elif show_tables=="COMMENTS":  
        show_comments_table() 

