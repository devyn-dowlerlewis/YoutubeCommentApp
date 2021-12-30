import requests
import pandas as pd
import time
import psycopg2 as ps

def pull_uploads_info(API_KEY, CHANNEL_ID, df, pages):
    # Build URL and make api call to gather channel information
    channel_url = "https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=" + CHANNEL_ID + "&key=" + API_KEY
    response1 = requests.get(channel_url).json()

    # Extract uploads playlist id from API return and build new URL
    PLAYLIST_ID = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    uploads_url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId=" + PLAYLIST_ID + "&key=" + API_KEY
    response2 = requests.get(uploads_url).json()

    if (pages < 1):
        pages = 1

    # Collect video information from first page (50 videos) and append to dataframe
    pageToken = response2['nextPageToken']
    for video in response2['items']:
        if video['kind'] == "youtube#playlistItem":
            video_id = video['snippet']['resourceId']['videoId']
            video_title = video['snippet']['title']
            video_title = str(video_title).replace("&#39;", "'")
            video_title = str(video_title).replace("&quot;", "'")
            channel_title = video['snippet']['channelTitle']
            upload_date = video['snippet']['publishedAt']
            # format date
            upload_date = str(upload_date).replace("T", " ")
            upload_date = str(upload_date).replace("Z", " UTC")

            # Additional API call needed to get video stats
            view_count, like_count, comment_count = get_video_details(video_id, API_KEY)

            df = df.append({'video_id': video_id, 'video_title': video_title, 'channel_title': channel_title,
                            'upload_date': upload_date,
                            'view_count': view_count, 'like_count': like_count,
                            'comment_count': comment_count}, ignore_index=True)

    # Add next page token to url and get next page of videos, loop to get desired number of pages
    for i in range(pages - 1):
        uploads_url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId=" + PLAYLIST_ID + "&key=" + API_KEY + "&pageToken=" + pageToken

        paged_response = requests.get(uploads_url).json()
        # print(paged_response)
        pageToken = paged_response['nextPageToken']
        # Exception may be thrown if a channel runs out of videos or API key reaches maximum allowed queries.
        try:
            for video in paged_response['items']:
                if video['kind'] == "youtube#playlistItem":
                    video_id = video['snippet']['resourceId']['videoId']
                    video_title = video['snippet']['title']
                    video_title = str(video_title).replace("&#39;", "'")
                    video_title = str(video_title).replace("&quot;", "'")
                    upload_date = video['snippet']['publishedAt']
                    upload_date = str(upload_date).replace("T", " ")
                    upload_date = str(upload_date).replace("Z", " UTC")

                    view_count, like_count, comment_count = get_video_details(video_id, API_KEY)

                    df = df.append({'video_id': video_id, 'video_title': video_title, 'channel_title': channel_title,
                                    'upload_date': upload_date,
                                    'view_count': view_count, 'like_count': like_count,
                                    'comment_count': comment_count}, ignore_index=True)
            print(i)
        except:
            print("error")
            print(i)

    return df


def get_video_details(video_id, API_KEY):
    # Build URL
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id=" + video_id + "&part=statistics&key=" + API_KEY
    reponse_video_stats = requests.get(url_video_stats).json()
    # print(reponse_video_stats)
    # print(reponse_video_stats)
    # Extract video information
    view_count = reponse_video_stats['items'][0]['statistics']['viewCount']
    like_count = reponse_video_stats['items'][0]['statistics']['likeCount']
    # Check if comments are disabled
    try:
        comment_count = reponse_video_stats['items'][0]['statistics']['commentCount']
    except:
        comment_count = 0

    return view_count, like_count, comment_count


def Extract_Parent_Comments(API_KEY, videoId, comdf, df):
    pageToken = ""
    # Loop through pages of comments and collect information
    for i in range(100):
        print(i)
        # Function to call api and return response containing 1 page of comments
        out = getCommentPage(API_KEY, videoId, pageToken)

        # Extract comment text and information
        # Break if API call returns any issues
        try:
            for comment in out['items']:
                ytCom = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                comAuthor = comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
                comDisplayName = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
                upload_date = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                # format date
                upload_date = str(upload_date).replace("T", " ")
                upload_date = str(upload_date).replace("Z", " UTC")
                # generate unique identifier for each comment
                key = videoId[0:2] + comAuthor[3:5] + upload_date[11:19]
                comdf = comdf.append(
                    {'key': key, 'videoId': videoId, 'author': comAuthor, 'display_name': comDisplayName,
                     'comment': ytCom, 'like_count': like_count, 'upload_date': upload_date}, ignore_index=True)
        except Exception as e:
            print(e)
            break
        # If nextpagetoken no longer exists we're out of comments
        try:
            pageToken = out['nextPageToken']
        except:
            print("out of comments")
            break

    return comdf


# Function to call api and return response containing 1 page of comments
def getCommentPage(API_KEY, videoId, pageToken):
    URL = "https://www.googleapis.com/youtube/v3/commentThreads?key=" + API_KEY + "&textFormat=plainText&part=snippet&videoId=" + videoId + "&maxResults=50" + "&pageToken=" + pageToken

    response = requests.get(URL).json()

    return response