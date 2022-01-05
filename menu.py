import pandas as pd
import concurrent.futures
from itertools import repeat

import ScrapeVideos
import DatabaseFunctions
import requests

THREADING_FLAG = True

#key = ""

def scrape_videos_comments(API_KEY, CHANNEL_ID, pages):
    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                               "comment_count"])

    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    df = ScrapeVideos.pull_uploads_info(API_KEY, CHANNEL_ID, df, pages)
    j = 0

    if THREADING_FLAG == False:
        for video in df['video_id']:
            comdf = ScrapeVideos.Extract_Parent_Comments(API_KEY, video, comdf)
            j = j + 1
            print("comments from video ", j)
        ScrapeVideos.reset_video_count()
        return df, comdf
    else:
        culcomdf = pd.DataFrame(
            columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
        tempcomdf = pd.DataFrame(
            columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

        print("Attempting to collect comments with multithreading")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(ScrapeVideos.Extract_Parent_Comments, repeat(API_KEY), df['video_id'], repeat(tempcomdf))
            for result in results:
                culcomdf = pd.concat([culcomdf, result], axis=0)

        ScrapeVideos.reset_video_count()
        return df, culcomdf


#         for video in df['video_id']:
#             tempcomdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
#             tempcomdf = Extract_Parent_Comments(API_KEY, video, tempcomdf, df)

#             culcomdf = pd.concat([culcomdf, tempcomdf], axis=0)
#             j = j + 1
#             print("comments from video ", j)
#         T3F = time.perf_counter()


def eliminate_duplicates(df, comdf, keep, silent=False):
    if keep:
        a = len(df)
        b = len(comdf)

        newdf = df.drop_duplicates(subset=['video_id'], keep='first')
        newcomdf = comdf.drop_duplicates(subset=['key'], keep='first')

        c = len(newdf)
        d = len(newcomdf)
        if silent:
            pass
        else:
            print(f"{a - c} DUPLICATE VIDEOS ELIMINATED")
            print(f"{b - d} DUPLICATE COMMENTS ELIMINATED")
    else:
        newdf = df.drop_duplicates(subset=['video_id'], keep=False)
        newcomdf = comdf.drop_duplicates(subset=['key'], keep=False)

    newdf = newdf.reset_index(drop=True)
    newcomdf = newcomdf.reset_index(drop=True)

    return newdf, newcomdf


def upload_videos_comments(df, comdf):
    conn = None
    default_prompt = "Use Default Database? ([y]/n) "
    if input(default_prompt) == "n":
        host_name = input("Enter Hostname: ")
        dbname = input("Enter Database Name: ")
        port = input("Enter Port: ")
        username = input("Enter Username: ")
        password = input("Enter Password: ")
    else:
        host_name = 'database-ctvnews.casuycfmi5ss.us-east-2.rds.amazonaws.com'
        dbname = 'russia'
        port = '5432'
        print("WRITING TO DEFAULT DATABASE REQUIRES EXTENDED PERMISSIONS")
        username = input("Enter Username: ")
        password = input("Enter Password: ")


    conn = DatabaseFunctions.connect_to_db(host_name, dbname, username, password, port)
    if conn is None:
        return df, comdf

    curr = conn.cursor()



    try:
        df, comdf = eliminate_duplicates(df, comdf, keep=True, silent=True)
        DatabaseFunctions.create_table(curr)
        new_vid_df = DatabaseFunctions.update_db(curr, df)
        DatabaseFunctions.append_from_df_to_db(curr, new_vid_df)
        print(f"Successfully Uploaded {len(df)} Videos")

        DatabaseFunctions.create_comment_table(curr)
        new_comment_df, existing_comdf = DatabaseFunctions.sort_comment_db(conn, comdf)
        #new_comment_df = DatabaseFunctions.update_comment_db(curr, existing_comdf)
        DatabaseFunctions.append_new_comments(curr, new_comment_df)
        #DatabaseFunctions.append_from_comdf_to_db(curr, new_comment_df)
        print(f"Successfully Uploaded {len(new_comment_df)} Comments")
    except Exception as e:
        print(e)
        print("No write privileges")
        return df, comdf

    conn.commit()
    print("Changes successfully committed to database.")



    if input('Wipe Memory? (y/(n)') == "y":
        df, comdf = reset_memory()
    else:
        pass
    return df, comdf


def download_videos_comments():
    conn = None
    host_name = 'database-ctvnews.casuycfmi5ss.us-east-2.rds.amazonaws.com'
    dbname = 'russia'
    port = '5432'
    username = 'publictest'
    password = '12345'

    conn = DatabaseFunctions.connect_to_db(host_name, dbname, username, password, port)
    curr = conn.cursor()

    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                               "comment_count"])
    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    df = pd.read_sql_query('select * from "videos"', conn)
    print(f"Downloaded information from {len(df)} videos")
    comdf = pd.read_sql_query('select * from "comments"', conn)
    print(f"Downloaded information from {len(comdf)} comments")

    return df, comdf

def toggle_threading():
    global THREADING_FLAG
    if THREADING_FLAG:
        THREADING_FLAG = False
    else:
        THREADING_FLAG = True
    print(f"THREADING = {THREADING_FLAG}")

def generate_prompt(df_length, com_length):
    prompt = f"""WELCOME TO MY MEDIOCRE YOUTUBE SCRAPER
bugs guaranteed...
___________________________________________________________________
CURRENTLY IN MEMORY: {"{:,}".format(df_length)} Videos  |  {"{:,}".format(com_length)} Comments
___________________________________________________________________
PRESS 1 TO SCRAPE VIDEOS AND COMMENTS FROM A TARGET CHANNEL

PRESS 2 TO SCRAPE COMMENTS FROM A TARGET VIDEO

PRESS 3 TO DOWNLOAD DATABASE COMMENTS AND VIDEOS TO MEMORY

PRESS 4 TO PRINT CURRENT VIDEO AND COMMENT DATA

PRESS 5 TO UPLOAD ALL VIDEO AND COMMENT INFORMATION TO A DATABASE (DEFAULT DATABASE REQUIRES ELEVATED PERMISSIONS)

PRESS 6 TO ELIMINATE ALL DUPLICATE ENTRIES IN MEMORY

PRESS 7 TO ELIMINATE ALL ENTRIES IN MEMORY ALREADY IN THE DATABASE

PRESS 8 TO WIPE EVERYTHING FROM MEMORY

PRESS 9 TO TOGGLE THREADING (CURRENTLY {THREADING_FLAG})
___________________________________________________________________
PRESS 0 TO EXIT
___________________________________________________________________
"""

    return prompt

def scrape(cul_df, cul_comdf, API_KEY):
    pages = input("How many pages of 50 videos?") or 1
    pages = int(pages)
    CHANNEL_ID = input("Enter a channel id (null entry defaults to CTV News): ") or "UCi7Zk9baY1tvdlgxIML8MXg"
    print("working...")

    df, comdf = scrape_videos_comments(API_KEY, CHANNEL_ID, pages)

    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    print(f"Scraped {len(df)} videos and {len(comdf)} comments.")

    return cul_df, cul_comdf

def print_data(cul_df, cul_comdf):
    print(cul_df)
    input("Press any key to show comment table")
    print()
    print(cul_comdf)

def download_to_memory(cul_df, cul_comdf):
    df, comdf = download_videos_comments()
    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    return cul_df, cul_comdf

def reset_memory(silent=False):
    cul_df = pd.DataFrame(
        columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                 "comment_count"])
    cul_comdf = pd.DataFrame(
        columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
    if silent:
        pass
    else:
        print("Memory Wiped")
    return cul_df, cul_comdf

def eliminate_dbclones(cul_df, cul_comdf, silent=False):
    conn = None
    default_prompt = "Use Default Database? ([y]/n)"
    if input(default_prompt) == "n":
        host_name = input("enter hostname")
        dbname = input("enter database name")
        port = input("enter port")
        username = input("enter username")
        password = input("enter password")
    else:
        host_name = 'database-ctvnews.casuycfmi5ss.us-east-2.rds.amazonaws.com'
        dbname = 'russia'
        port = '5432'
        username = 'publictest'
        password = '12345'

    conn = DatabaseFunctions.connect_to_db(host_name, dbname, username, password, port)
    curr = conn.cursor()

    key_df = pd.read_sql_query('select video_id from "videos"', conn)
    key_df = key_df['video_id'].tolist()
    key_comdf = pd.read_sql_query('select key from "comments"', conn)
    key_comdf = key_comdf['key'].tolist()

    new_df = cul_df[~cul_df.video_id.isin(key_df)]
    new_comdf = cul_comdf[~cul_comdf.key.isin(key_comdf)]

    if silent:
        pass
    else:
        a = len(cul_df)
        c = len(new_df)
        b = len(cul_comdf)
        d = len(new_comdf)
        print(f"{a - c} EXISTING VIDEOS ELIMINATED")
        print(f"{b - d} EXISTING COMMENTS ELIMINATED")

    return new_df, new_comdf

def scrape_one(cul_df, cul_comdf, API_KEY):
    pages = input("How many pages of 50 comments?") or 1
    pages = int(pages)
    VIDEO_ID = input("Enter a video id (null entry defaults to... just enter one.): ") or "dQw4w9WgXcQ"
    print("working...")
    df = scrape_single_video_info(VIDEO_ID, API_KEY)
    comdf = scrape_comments(API_KEY, VIDEO_ID, pages)

    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    print(f"Scraped {len(comdf)} comments.")
    ScrapeVideos.reset_video_count()

    return cul_df, cul_comdf


def scrape_comments(API_KEY, VIDEO_ID, pages):
    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
    comdf = ScrapeVideos.Extract_Parent_Comments(API_KEY, VIDEO_ID, comdf, pages, keep_updated=True)
    return comdf

def scrape_single_video_info(VIDEO_ID, API_KEY):
    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count", "comment_count"])

    url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet&id={VIDEO_ID}&key={API_KEY}'
    response1 = requests.get(url).json()

    video_id = VIDEO_ID
    video_title = response1['items'][0]['snippet']['title']
    upload_date = response1['items'][0]['snippet']['publishedAt']
    channel_title = response1['items'][0]['snippet']['channelTitle']

    view_count, like_count, comment_count = ScrapeVideos.get_video_details(video_id, API_KEY)
    df = df.append({'video_id': video_id, 'video_title': video_title, 'channel_title': channel_title,
                    'upload_date': upload_date,
                    'view_count': view_count, 'like_count': like_count,
                    'comment_count': comment_count}, ignore_index=True)

    return df

