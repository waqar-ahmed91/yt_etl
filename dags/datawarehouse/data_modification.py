import logging

logger = logging.getLogger(__name__)
table = 'yt_api'

def insert_rows(cur, conn, schema, row):
    try:
        if schema =='staging':

            video_id = 'video_id'

            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID","Video_Title","Upload_Date","Duration","Video_Views","Likes_Counts", "Comment_Counts")

                    VALUES (%(video_id)s,%(title)s,%(publishedAt)s,%(duration)s,%(viewCount)s,%(likeCount)s,%(commentCount)s)
                """, row
            )

        else:

            video_id = "Video_ID"

            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID","Video_Title","Upload_Date","Duration","Video_Type","Video_Views","Likes_Counts", "Comment_Counts")

                    VALUES (%(Video_ID)s,%(Video_Title)s,%(Upload_Date)s,%(Duration)s,%(Video_Type)s,%(Video_Views)s,%(Likes_Counts)s,%(Comment_Counts)s)
                """, row
            )

        conn.commit()

        logger.info(f"Inserted Rows with Video ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting rows with video ID:{row[video_id]}")
        raise e

def update_rows(cur, conn, schema, row):

    try:
        if schema == 'staging':
            video_id =  'video_id'
            video_title = 'title'
            upload_date = 'publishedAt'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'

        else:
            video_id =  'Video_ID'
            video_title = 'Video_Title'
            upload_date = 'Upload_Date'
            video_views = 'Video_Views'
            likes_count = 'Likes_Counts'
            comments_count = 'Comment_Counts'

        cur.execute(f"""UPDATE {schema}.{table}
                        SET "Video_Title" = %({video_title})s,
                            "Video_Views" = %({video_views})s,
                            "Likes_Counts" = %({likes_count})s,
                            "Comment_Counts" = %({comments_count})s
                        WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
                    """, row
                    )
        conn.commit()

        logger.info(f"Updated row with Video ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with video ID:{row[video_id]}")
        raise e



def delete_rows(cur, conn, schema, ids_to_delete):

    try:
        ids_to_delete = f"""({','.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" in {ids_to_delete};
            """
        )

        conn.commit()

        logger.info(f"Deleted rows with Video ID: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with video ID:{ids_to_delete}")
        raise e