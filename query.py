import streamlit as st
import psycopg2
import pandas as pd

def app():
    mydb=psycopg2.connect(host="localhost",
                            user='postgres',
                            password='root',
                            database='youtube_data',
                            port='5432')
    cursor=mydb.cursor()
    question=st.selectbox("Select Your Question",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channel has the most number of videos, and how much do they have?",
                                                "3. What are the top 10 most viewed videos and their corresponding channel names?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the most number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022",
                                                "9. What is the average duration of all the videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))
    if question[0:2]=='1.':
        query1='''select title as videos,channel_name as channelname from videos'''
        cursor.execute(query1)
        mydb.commit()
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=['video tile','channel name'])
        st.write(df)
    elif question[0:2]=='2.':
        query2='''select channel_name as channelname,total_videos as no_videos from channels
            order by total_videos desc'''   
        cursor.execute(query2)
        mydb.commit()
        t2=cursor.fetchall()
        df2=pd.DataFrame(t2,columns=['channel name','No of Videos'])
        st.write(df2)
    elif question[0:2]=='3.':
        query3='''select views as views,channel_name as channelname,title as videotitle from videos where views is not null
            order by views desc limit 10'''   
        cursor.execute(query3)
        mydb.commit()
        t3=cursor.fetchall()
        df3=pd.DataFrame(t3,columns=['Views','Channel name','Video title'],index=[1,2,3,4,5,6,7,8,9,10])
        st.write(df3)  
    elif question[0:2]=='4.':
        query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''   
        cursor.execute(query4)
        mydb.commit()
        t4=cursor.fetchall()
        df4=pd.DataFrame(t4,columns=['No of Comments','Video Title'])
        st.write(df4)
    elif question[0:2]=='5.':
        query5='''select title as videotitle, channel_name as channelname,likes as likecount from videos
                where likes is not null order by likes desc'''   
        cursor.execute(query5)
        mydb.commit()
        t5=cursor.fetchall()
        df5=pd.DataFrame(t5,columns=['Video Title','Channel Name','Like Count'])
        st.write(df5)
    elif question[0:2]=='6.':
        query6='''select likes as likecount,title as videotitle from videos'''   
        cursor.execute(query6)
        mydb.commit()
        t6=cursor.fetchall()
        df6=pd.DataFrame(t6,columns=['Like Count','Video Title'])
        st.write(df6)
    elif question[0:2]=='7.':
        query7='''select channel_name as channelname,views as totalviews from channels'''   
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        df7=pd.DataFrame(t7,columns=['Channel Name','Total Views'])
        st.write(df7)
    elif question[0:2]=='8.':
        query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''   
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        df8=pd.DataFrame(t8,columns=['Video Title','Published Date','Channel Name'])
        st.write(df8)
    elif question[0:2]=='9.':
        query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''   
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=['Channel Name','Average Duration'])
        T9=[]
        for index,row in df9.iterrows():
            channel_title=row['Channel Name']
            average_duration=row['Average Duration']
            average_duration_str=str(average_duration)
            T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T9)
        st.write(df9)
    elif question[0:2]=='10':    
        query10='''select title as video_title,channel_name as channelname,comments as comments from videos
                where comments is not null order by comments desc'''   
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=['Video Title','Published Date','Channel Name'])
        st.write(df10)
