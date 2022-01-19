import pandas as pd
import psycopg2 as ps
from psycopg2.extras import execute_values
import time
import menu
from tabulate import tabulate

# Connect to the Database
def connect_to_db(type = 'default'):
    conn = None
    if type == 'custom':
        host_name = input("Enter Hostname: ")
        dbname = input("Enter Database Name: ")
        port = input("Enter Port: ")
        username = input("Enter Username: ")
        password = input("Enter Password: ")
    elif type == 'elevated':
        host_name = 'database-ctvnews.casuycfmi5ss.us-east-2.rds.amazonaws.com'
        dbname = 'russia'
        port = '5432'
        print("EXTENDED PERMISSIONS REQUIRED")
        username = input("Enter Username: ")
        password = input("Enter Password: ")
    else:
        host_name = 'database-ctvnews.casuycfmi5ss.us-east-2.rds.amazonaws.com'
        dbname = 'russia'
        port = '5432'
        username = 'publictest'
        password = '12345'

    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
        curr = conn.cursor()
    except ps.OperationalError as e:
        print(f"Could not connect to database. Error: {e}")
        conn = None
        curr = None
    else:
        print('Connected!')
    return conn, curr


# Create the video table in the database
def create_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS videos (
                        video_id VARCHAR(255) PRIMARY KEY,
                        video_title TEXT NOT NULL,
                        channel_title TEXT NOT NULL,
                        upload_date TIMESTAMP NOT NULL,
                        view_count INTEGER NOT NULL,
                        like_count INTEGER NOT NULL,
                        comment_count INTEGER NOT NULL
                )""")
    curr.execute(create_table_command)


# If a video is already in the database, update the stats
def update_db(curr, df):
    # Dataframe to hold video's that don't yet exist in the DB
    temp_df = pd.DataFrame(
        columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                 "comment_count"])

    for i, row in df.iterrows():
        if check_if_video_exists(curr, row['video_id']):
            update_row(curr, row['video_id'], row['video_title'], row['view_count'], row['like_count'],
                       row['comment_count'])
        else:
            temp_df = temp_df.append(row)
    return temp_df


# Return true if a video is in the database
def check_if_video_exists(curr, video_id):
    query = ("""SELECT video_id FROM VIDEOS WHERE video_id = %s""")
    curr.execute(query, (video_id,))

    return curr.fetchone() is not None


# Update a row with inputs
def update_row(curr, video_id, video_title, view_count, like_count, comment_count):
    query = ("""UPDATE videos
            SET video_title = %s,
                view_count = %s,
                like_count = %s,
                comment_count = %s
            WHERE video_id = %s;""")
    vars_to_update = (video_title, view_count, like_count, comment_count, video_id)
    curr.execute(query, vars_to_update)


# Loop through temp dataframe and insert new videos into the database table
def append_from_df_to_db(curr, df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['video_id'], row['video_title'], row['channel_title'], row['upload_date'],
                          row['view_count'], row['like_count'], row['comment_count'])


# Insert a video
def insert_into_table(curr, video_id, video_title, channel_title, upload_date, view_count, like_count, comment_count):
    insert_into_videos = ("""INSERT INTO videos (video_id, video_title, channel_title, upload_date, view_count, like_count, comment_count)
        VALUES(%s,%s,%s,%s,%s,%s,%s);""")

    row_to_insert = (video_id, video_title, channel_title, upload_date, view_count, like_count, comment_count)
    curr.execute(insert_into_videos, row_to_insert)


# Create the comment table in the database
def create_comment_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS comments (
                        key VARCHAR(255) PRIMARY KEY,
                        videoid VARCHAR(255) NOT NULL,
                        author VARCHAR(255) NOT NULL,
                        display_name TEXT NOT NULL,
                        comment TEXT NOT NULL,
                        like_count INTEGER NOT NULL,
                        upload_date TIMESTAMP NOT NULL
                )""")
    curr.execute(create_table_command)


def sort_comment_db(conn, comdf):
    key_comdf = pd.read_sql_query('select key from "comments"', conn)
    key_comdf = key_comdf['key'].tolist()
    new_comdf = comdf[~comdf.key.isin(key_comdf)]
    old_comdf = comdf[comdf.key.isin(key_comdf)]
    #print(f"Sorted comments into {len(new_comdf)} New comments and {len(old_comdf)} previously existing comments.")
    print(f"Removed {len(old_comdf)} previously existing comments.")
    new_comdf = new_comdf.reset_index(drop=True)
    old_comdf = old_comdf.reset_index(drop=True)
    return new_comdf, old_comdf


def update_comment_db(curr, comdf):
    temp_df = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])
    up_count = 0
    new_count = 0
    for i, row in comdf.iterrows():
        if check_if_comment_exists(curr, row['key']):
            update_comment_row(curr, row['key'], row['videoid'], row['author'], row['display_name'], row['comment'],
                               row['like_count'], row['upload_date'])
            up_count = up_count + 1
        else:
            temp_df = temp_df.append(row)
            new_count = new_count + 1
        if (i % 100 == 0):
            print('Comment: ', i + 1)
            print('New Comment Count: ', new_count)
            print('Old Comment Count: ', up_count)
            print()
    return temp_df


def update_comment_row(curr, key, i, author, display_name, comment, like_count, upload_date):
    query = ("""UPDATE comments
        SET display_name = %s,
            comment = %s,
            like_count = %s
        WHERE key = %s;""")
    vars_to_update = (display_name, comment, like_count, key)
    curr.execute(query, vars_to_update)


def check_if_comment_exists(curr, key):
    query = ("""SELECT key FROM comments WHERE key = %s""")
    curr.execute(query, (key,))

    return curr.fetchone() is not None


def append_new_comments(curr, comdf):
    print(f"Uploading comments. Estimated time to upload {len(comdf)} comments: {round(len(comdf) / 1600, 5)} seconds")
    start = time.perf_counter()
    records = comdf.to_records(index=False)
    comdf_tuple = list(records)
    execute_values(curr, "INSERT INTO comments (key, videoid, author, display_name, comment, like_count, upload_date) VALUES %s", comdf_tuple)
    end = time.perf_counter()
    print(f"Finished in {round(end - start, 6)} seconds. This corresponds to {(round(end - start, 6) / len(comdf)) * 1000} ms per comment.")


def append_from_comdf_to_db(curr, comdf):
    i = 0
    start = time.perf_counter()
    print(f"Uploading {len(comdf)} new comments to the database")
    for i, row in comdf.iterrows():
        insert_comment_into_table(curr, row['key'], row['videoid'], row['author'], row['display_name'], row['comment'],
                                  row['like_count'], row['upload_date'])
        if ((i + 1) % 500 == 0):
            print('New Comments Inserted: ', i + 1)
    end = time.perf_counter()
    print(f"Finished in {round(end - start, 6)} seconds. This corresponds to {(round(end - start, 6) / len(comdf)) * 1000} ms per comment.")

def insert_comment_into_table(curr, key, videoid, author, display_name, comment, like_count, upload_date):
    insert_into_comments = ("""INSERT INTO comments (key, videoid, author, display_name, comment, like_count, upload_date)
        VALUES(%s,%s,%s,%s,%s,%s,%s);""")

    row_to_insert = (key, videoid, author, display_name, comment, like_count, upload_date)

    try:
        curr.execute(insert_into_comments, row_to_insert)
    except:
        print("ERROR FAILED TO UPLOAD A COMMENT:")
        print(row_to_insert)
        input("Press any key to continue")

#Download_Menu_________________________________________________________________

def download_channel_videos_comments(target):
    conn, curr = connect_to_db()

    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                               "comment_count"])
    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    sql_df = f'''SELECT videos.* FROM videos 
LEFT JOIN comments ON videos.video_id = comments.videoid WHERE videos.channel_title = '{target}';'''

    sql_comdf = f'''SELECT comments.* FROM videos 
JOIN comments ON videos.video_id = comments.videoid WHERE videos.channel_title = '{target}';'''
    comdf = pd.read_sql_query(sql_comdf, conn)

    df = pd.read_sql_query(sql_df, conn)
    df, comdf = menu.eliminate_duplicates(df, comdf, keep=True, silent=True)
    print(f"Downloaded information from {len(df)} videos")
    print(f"Downloaded information from {len(comdf)} comments")
    return df, comdf


def download_all_videos_comments():
    conn, curr = connect_to_db()

    df = pd.DataFrame(columns=["video_id", "video_title", "channel_title", "upload_date", "view_count", "like_count",
                               "comment_count"])
    comdf = pd.DataFrame(columns=['key', 'videoid', 'author', 'display_name', 'comment', 'like_count', 'upload_date'])

    df = pd.read_sql_query('select * from "videos"', conn)
    print(f"Downloaded information from {len(df)} videos")
    comdf = pd.read_sql_query('select * from "comments"', conn)
    print(f"Downloaded information from {len(comdf)} comments")

    return df, comdf

def display_contents(prompt):
    video_count_sql = '''SELECT channel_title, COUNT(*) FROM videos GROUP BY channel_title order by channel_title;'''
    comment_count_sql = '''SELECT videos.channel_title, COUNT(*) FROM comments 
    JOIN videos ON comments.videoid = videos.video_id GROUP BY channel_title order by channel_title;'''

    conn, curr = connect_to_db()

    count_df = pd.read_sql_query(video_count_sql, conn)
    count_comdf = pd.read_sql_query(comment_count_sql, conn)
    count_df['countcoms'] = count_comdf['count']
    df = count_df.sort_values(by=['countcoms'], ascending=False, ignore_index=True)

    print(prompt)
    print(tabulate(df, headers=["CHANNEL NAME", "VIDEO COUNT", "COMMENT COUNT"], tablefmt='psql', showindex=False))

#Upload_________________________________________________________________________________________________________________________

def upload_db_info(df, comdf, curr, conn):
    if conn is None:
        return df, comdf

    try:
        df, comdf = menu.eliminate_duplicates(df, comdf, keep=True, silent=True)
        create_table(curr)
        new_vid_df = update_db(curr, df)
        append_from_df_to_db(curr, new_vid_df)
        print(f"Successfully Uploaded {len(df)} Videos")

        create_comment_table(curr)
        new_comment_df, _ = sort_comment_db(conn, comdf)
        append_new_comments(curr, new_comment_df)
        print(f"Successfully Uploaded {len(new_comment_df)} Comments")
    except Exception as e:
        print(e)
        print("No write privileges")
        return df, comdf

    conn.commit()
    print("Changes successfully committed to database.")
    return df, comdf