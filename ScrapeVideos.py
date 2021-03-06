import requests
import pandas as pd
import concurrent.futures
from itertools import repeat

VIDEO_COUNT = 0

def scrape_single_video_info(VIDEO_ID, API_KEY):
    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count", "comment_count"])

    url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet&id={VIDEO_ID}&key={API_KEY}'
    response1 = requests.get(url).json()

    video_id = VIDEO_ID
    video_title = response1['items'][0]['snippet']['title']
    upload_date = response1['items'][0]['snippet']['publishedAt']
    channel_title = response1['items'][0]['snippet']['channelTitle']

    view_count, like_count, comment_count = get_video_details(video_id, API_KEY)
    df = df.append({'video_id': video_id, 'video_title': video_title, 'channel_title': channel_title,
                    'upload_date': upload_date,
                    'view_count': view_count, 'like_count': like_count,
                    'comment_count': comment_count}, ignore_index=True)
    reset_video_count()
    return df

def scrape_videos_comments(API_KEY, CHANNEL_ID, pages):
    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                               "comment_count"])

    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    df = pull_uploads_info(API_KEY, CHANNEL_ID, df, pages)
    j = 0


    culcomdf = pd.DataFrame(
        columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
    tempcomdf = pd.DataFrame(
        columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    print("Attempting to collect comments with multithreading")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(Extract_Parent_Comments, repeat(API_KEY), df['video_id'], repeat(tempcomdf))
        for result in results:
            culcomdf = pd.concat([culcomdf, result], axis=0)

    reset_video_count()
    return df, culcomdf

def pull_uploads_info(API_KEY, CHANNEL_ID, df, pages):
    vids_done = 0
    pageToken = ""
    # Build URL and make api call to gather channel information
    channel_url = "https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=" + CHANNEL_ID + "&key=" + API_KEY
    response1 = requests.get(channel_url).json()

    # Extract uploads playlist id from API return and build new URL
    try:
        PLAYLIST_ID = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as ex:
        print(f"Issue with API request: {ex}")
        return df

    if (pages < 1):
        pages = 1

    for i in range(pages):
        uploads_url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId=" + PLAYLIST_ID + "&key=" + API_KEY + "&pageToken=" + pageToken
        paged_response = requests.get(uploads_url).json()
        if i == 0:
            print(f"Selected Channel: {paged_response['items'][0]['snippet']['channelTitle']}")

        for video in paged_response['items']:
            if video['kind'] == "youtube#playlistItem":
                vids_done += 1

                video_id = video['snippet']['resourceId']['videoId']
                video_title = video['snippet']['title']
                video_title = str(video_title).replace("&#39;", "'")
                video_title = str(video_title).replace("&quot;", "'")
                upload_date = video['snippet']['publishedAt']
                upload_date = str(upload_date).replace("T", " ")
                upload_date = str(upload_date).replace("Z", " UTC")
                channel_title = video['snippet']['channelTitle']

                view_count, like_count, comment_count = get_video_details(video_id, API_KEY)

                df = df.append({'video_id': video_id, 'video_title': video_title, 'channel_title': channel_title,
                                'upload_date': upload_date,
                                'view_count': view_count, 'like_count': like_count,
                                'comment_count': comment_count}, ignore_index=True)
                if vids_done % 10 == 0:
                    print(f"Recorded information from {vids_done} videos")
        try:
            pageToken = paged_response['nextPageToken']
        except:
            print("Channel Out of Videos")
            return df

    return df


def get_video_details(video_id, API_KEY):
    # Build URL
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id=" + video_id + "&part=statistics&key=" + API_KEY
    reponse_video_stats = requests.get(url_video_stats).json()
    # print(reponse_video_stats)
    # Extract video information

    # Pretty sure all videos have public view counts but just in case...
    try:
        view_count = reponse_video_stats['items'][0]['statistics']['viewCount']
    except:
        print('error no view information')
        print(reponse_video_stats)
        view_count = 0
    # Check if rating disabled
    try:
        like_count = reponse_video_stats['items'][0]['statistics']['likeCount']
    except:
        print('error no like_count information')
        print(reponse_video_stats)
        like_count = 0
    # Check if comments are disabled
    try:
        comment_count = reponse_video_stats['items'][0]['statistics']['commentCount']
    except:
        comment_count = 0

    return view_count, like_count, comment_count


def Extract_Parent_Comments(API_KEY, videoid, comdf, pages=421, keep_updated=False):
    pageToken = ""
    count = 0
    # global total_api_time
    # Loop through pages of comments and collect information
    for i in range(pages):

        # Function to call api and return response containing 1 page of comments
        out = getCommentPage(API_KEY, videoid, pageToken)


        # Extract comment text and information
        # Break if API call returns any issues
        try:
            for comment in out['items']:
                ytCom = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                ytCom = ytCom.replace('\x00', '')
                comAuthor = comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
                comDisplayName = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
                upload_date = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                # format date
                upload_date = str(upload_date).replace("T", " ")
                upload_date = str(upload_date).replace("Z", " UTC")
                # generate unique identifier for each comment
                key = videoid[0:2] + comAuthor[3:5] + upload_date[11:19]
                comdf = comdf.append(
                    {'key': key, 'videoid': videoid, 'author': comAuthor, 'display_name': comDisplayName,
                     'comment': ytCom, 'like_count': like_count, 'upload_date': upload_date}, ignore_index=True)
        except Exception as e:
            #print(e)
            pass
        # If nextpagetoken no longer exists we're out of comments
        # d = time.perf_counter()
        try:
            pageToken = out['nextPageToken']
        except Exception as e:
            #print(e)
            break
        if keep_updated:
            print(f"Collected page {(i + 1)} of comments.")
        # print(f"get comment page took {c-b} seconds to run. This consitutes {((c-b)/(d-b))*100}% of the total loop time")

    global VIDEO_COUNT
    VIDEO_COUNT += 1
    print(f"Finished Extracting Comments from {VIDEO_COUNT} videos\n")

    return comdf

def reset_video_count():
    global VIDEO_COUNT
    VIDEO_COUNT = 0



# Function to call api and return response containing 1 page of comments
def getCommentPage(API_KEY, videoid, pageToken):
    URL = "https://www.googleapis.com/youtube/v3/commentThreads?key=" + API_KEY + "&textFormat=plainText&part=snippet&videoId=" + videoid + "&maxResults=50" + "&pageToken=" + pageToken

    response = requests.get(URL).json()

    return response