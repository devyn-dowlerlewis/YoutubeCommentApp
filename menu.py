import pandas as pd
import concurrent.futures
from itertools import repeat

import ScrapeVideos
import DatabaseFunctions
import Analysis

import requests
import os
from tabulate import tabulate


def generate_main_prompt(df_length, com_length):
    prompt = f"""WELCOME TO MY MEDIOCRE YOUTUBE SCRAPER
bugs guaranteed...
___________________________________________________________________
CURRENTLY IN MEMORY: {"{:,}".format(df_length)} Videos  |  {"{:,}".format(com_length)} Comments
___________________________________________________________________
PRESS 1 TO SCRAPE VIDEOS AND COMMENTS FROM A TARGET CHANNEL

PRESS 2 TO SCRAPE COMMENTS FROM A TARGET VIDEO

PRESS 3 TO DOWNLOAD INFORMATION FROM A DATABASE

PRESS 4 TO PRINT CURRENT VIDEO AND COMMENT DATA

PRESS 5 TO UPLOAD ALL VIDEO AND COMMENT INFORMATION TO A DATABASE (DEFAULT DATABASE REQUIRES ELEVATED PERMISSIONS)

PRESS 6 TO ELIMINATE ALL DUPLICATE ENTRIES IN MEMORY

PRESS 7 TO ELIMINATE ALL ENTRIES IN MEMORY ALREADY IN THE DATABASE

PRESS 8 TO WIPE EVERYTHING FROM MEMORY
___________________________________________________________________
PRESS 0 TO EXIT
___________________________________________________________________
"""

    return prompt

#SCRAPE FROM CHANNEL______________________________________________________________________________________________

def scrape(cul_df, cul_comdf, API_KEY):
    pages = input("How many pages of 50 videos?") or 1
    pages = int(pages)
    CHANNEL_ID = input("Enter a channel id (null entry defaults to CTV News): ") or "UCi7Zk9baY1tvdlgxIML8MXg"
    print("working...")

    df, comdf = ScrapeVideos.scrape_videos_comments(API_KEY, CHANNEL_ID, pages)

    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    print(f"Scraped {len(df)} videos and {len(comdf)} comments.")

    return cul_df, cul_comdf




#SCRAPE FROM VIDEO_______________________________________________________________________________________________________________________

def scrape_one(cul_df, cul_comdf, API_KEY):
    pages = input("How many pages of 50 comments?") or 1
    pages = int(pages)
    VIDEO_ID = input("Enter a video id (null entry defaults to... just enter one.): ") or "dQw4w9WgXcQ"
    print("working...")
    df = ScrapeVideos.scrape_single_video_info(VIDEO_ID, API_KEY)

    #comdf = scrape_comments(API_KEY, VIDEO_ID, pages)
    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
    comdf = ScrapeVideos.Extract_Parent_Comments(API_KEY, VIDEO_ID, comdf, pages, keep_updated=True)

    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    print(f"Scraped {len(comdf)} comments.")
    #ScrapeVideos.reset_video_count()

    return cul_df, cul_comdf


# def scrape_comments(API_KEY, VIDEO_ID, pages):
#     comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
#     comdf = ScrapeVideos.Extract_Parent_Comments(API_KEY, VIDEO_ID, comdf, pages, keep_updated=True)
#     return comdf

#DOWNLOAD MENU_________________________________________________________________________________________________________________________________________

def generate_download_prompt(df_length, com_length):
    prompt = f"""DOWNLOAD FROM DATABASE
___________________________________________________________________
CURRENTLY IN MEMORY: {"{:,}".format(df_length)} Videos  |  {"{:,}".format(com_length)} Comments
___________________________________________________________________
PRESS 1 TO VIEW DATABASE CONTENTS

PRESS 2 TO DOWNLOAD FROM A SPECIFIED CHANNEL

PRESS 3 TO DOWNLOAD ALL VIDEOS
___________________________________________________________________
PRESS 0 TO EXIT
___________________________________________________________________
"""
    return prompt


def generate_header_prompt(df_length, com_length):
    prompt = f'''___________________________________________________________________
    CURRENTLY IN MEMORY: {"{:,}".format(df_length)} Videos  |  {"{:,}".format(com_length)} Comments
    ___________________________________________________________________
    '''
    return prompt


def download_menu(cul_df, cul_comdf):
    while (user_input := input(generate_download_prompt(len(cul_df), len(cul_comdf)))) != "0":
        if user_input == "1":
            os.system('cls')
            cul_df, cul_comdf = display_DB_contents(cul_df, cul_comdf)
        elif user_input == "2":
            os.system('cls')
            target = input('please enter channel name: ')
            cul_df, cul_comdf = download_channel_to_memory(cul_df, cul_comdf, target)
            input("Press any key to continue")
        elif user_input == "3":
            os.system('cls')
            cul_df, cul_comdf = download_all_to_memory(cul_df, cul_comdf)
            input("Press any key to continue")
        else:
            os.system('cls')
            input(f"{user_input} IS DECIDEDLY NOT AN OPTION")

        os.system('cls')
    return cul_df, cul_comdf


def display_DB_contents(cul_df, cul_comdf):
    prompt = generate_header_prompt(len(cul_df), len(cul_comdf))
    DatabaseFunctions.display_contents(prompt)

    target = input("Please Specify a Channel To Download Videos From (0 to Exit): ")
    if target == '0':
        return cul_df, cul_comdf
    else:
        cul_df, cul_comdf = download_channel_to_memory(cul_df, cul_comdf, target)
        return cul_df, cul_comdf


def download_all_to_memory(cul_df, cul_comdf):
    df, comdf = DatabaseFunctions.download_all_videos_comments()
    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    return cul_df, cul_comdf


#Exception handling needs to be implemented for invalid target
def download_channel_to_memory(cul_df, cul_comdf, target):
    df, comdf = DatabaseFunctions.download_channel_videos_comments(target)
    cul_df = pd.concat([cul_df, df], axis=0)
    cul_comdf = pd.concat([cul_comdf, comdf], axis=0)
    return cul_df, cul_comdf

#Print Data_______________________________________________________________________________________________________________

#Improve this to make it semi-readable
def print_data(cul_df, cul_comdf):
    print(cul_df)
    input("Press any key to show comment table")
    print()
    print(cul_comdf)

#Upload to DB__________________________________________________________________________________________________________________________________________

def upload_videos_comments(df, comdf):
    default_prompt = "Use Default Database? ([y]/n) "
    if input(default_prompt) == "n":
        conn, curr = DatabaseFunctions.connect_to_db(type='custom')
    else:
        conn, curr = DatabaseFunctions.connect_to_db(type='elevated')

    df, comdf = DatabaseFunctions.upload_db_info(df, comdf, curr, conn)
    if input('Wipe Memory? (y/(n)') == "y":
        df, comdf = reset_memory()
    else:
        pass
    return df, comdf

#____________________________________________________________________________________________________________________________________________
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

#Analysis_Menu_____________________________________________________________________________________________________


def generate_analysis_prompt(df_length, com_length):
    prompt = f"""DATA ANALYSIS
___________________________________________________________________
CURRENTLY IN MEMORY: {"{:,}".format(df_length)} Videos  |  {"{:,}".format(com_length)} Comments
___________________________________________________________________
PRESS 1 TO ANALYSE COMMENT SENTIMENT
___________________________________________________________________
PRESS 0 TO EXIT
___________________________________________________________________
"""

    return prompt


def analysis_menu(cul_df, cul_comdf):
    df_anal = pd.DataFrame(columns=["video_id", "avg_sentiment"])
    comdf_anal = pd.DataFrame(columns=["key", "flair_sentiment_slow", "flair_sentiment_fast"])

    while (user_input := input(generate_analysis_prompt(len(cul_df), len(cul_comdf)))) != "0":
        if user_input == "1":
            os.system('cls')
            cul_df, cul_comdf, df_anal, comdf_anal = Analysis.analyse_sentiment(cul_df, cul_comdf, df_anal, comdf_anal)
            print(comdf_anal)
            input("Press any key to continue")
        else:
            os.system('cls')
            input(f"{user_input} IS DECIDEDLY NOT AN OPTION")
        os.system('cls')
    return cul_df, cul_comdf

#Wipe Memory_____________________________________________________________________________________________________________________

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

#Eliminate DB Clones________
# ____________________________________________________________________________________________________

def eliminate_dbclones(cul_df, cul_comdf, silent=False):
    conn = None
    default_prompt = "Use Default Database? ([y]/n)"
    if input(default_prompt) == "n":
        conn, curr = DatabaseFunctions.connect_to_db(type='custom')
    else:
        conn, curr = DatabaseFunctions.connect_to_db(type='default')

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

#Experimental Functions________________________________________________________________________________________________________________

def grab_missing_comments(df, cul_comdf):
    API_KEY = input("Please Enter Your Youtube API Key: ")
    id_comdf = cul_comdf['videoid'].tolist()
    new_df = df[~df.video_id.isin(id_comdf)]
    print(f"{len(new_df)} videos have no associated comments.")

    tempcomdf = pd.DataFrame(
        columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    print("Attempting to collect comments with multithreading")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(ScrapeVideos.Extract_Parent_Comments, repeat(API_KEY), new_df['video_id'], repeat(tempcomdf))
        for result in results:
            cul_comdf = pd.concat([cul_comdf, result], axis=0)

    ScrapeVideos.reset_video_count()
    return df, cul_comdf







