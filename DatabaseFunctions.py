import pandas as pd
import psycopg2 as ps

# Connect to the Database
def connect_to_db(host_name, dbname, username, password, port):
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    return conn


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


def append_from_comdf_to_db(curr, comdf):
    i = 0
    for i, row in comdf.iterrows():
        insert_comment_into_table(curr, row['key'], row['videoid'], row['author'], row['display_name'], row['comment'],
                                  row['like_count'], row['upload_date'])
        if ((i + 1) % 500 == 0):
            print('New Comments Inserted: ', i + 1)


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