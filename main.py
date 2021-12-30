import requests
import pandas as pd
import time
import psycopg2 as ps

import ScrapeVideos
import DatabaseFunctions

def scrape_videos_comments(API_KEY, CHANNEL_ID, pages):
    # Blank Dataframe to hold video info
    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                               "comment_count"])
    # Blank Dataframe to hold comment info
    comdf = pd.DataFrame(columns=['key', 'videoId', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    # Pages of videos to request from API. 1 page = 50 videos
    df = ScrapeVideos.pull_uploads_info(API_KEY, CHANNEL_ID, df, pages)
    j = 0
    for video in df['video_id']:
        comdf = ScrapeVideos.Extract_Parent_Comments(API_KEY, video, comdf, df)
        j = j + 1
        print("comments from video ", j)
    return df, comdf


#main
#CHANNEL_ID = "UCINg1S61mpN7dZW8vR2ikCw"
API_KEY = "AIzaSyB5n7zlrUZKYI4atZd2OY98QP1EqM8GAsE"


prompt = """
WELCOME TO MY SHITTY SCUFFED ASS YOUTUBE APP
by a stoned loser

PRESS 1 TO SCRAPE VIDEOS AND COMMENTS FROM A TARGET CHANNEL

PRESS 2 TO HEAR A FUN RACIAL SLUR

PRESS 3 TO PRINT CURRENT VIDEO DATA

PRESS 4 TO PRINT COMMENT DATA

PRESS 5 TO EXIT THIS PIECE OF SHIT
"""

while (user_input := input(prompt)) != "5":
    print()
    if user_input == "1":
        print()
        pages = int(input("How many pages of 50 videos?"))
        CHANNEL_ID= input("Enter a channel id (null entry defaults to CTV News): ") or "UCi7Zk9baY1tvdlgxIML8MXg"
        df, comdf = scrape_videos_comments(API_KEY, CHANNEL_ID, pages)
        print(f"Scraped {len(df)} videos and {len(comdf)} comments.")
        input("Press any key to continue")
    elif user_input == "2":
        input("YOU CRACKER ASS NIGGERFAGGOT")
    elif user_input == "3":
        print(df)
        input("Press any key to continue")
    elif user_input == "4":
        print(comdf)
        input("Press any key to continue")
    else:
        input("YOU DUMB FUCKING APE")




print("EXited")