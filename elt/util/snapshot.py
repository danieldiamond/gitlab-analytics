#!/usr/bin/python3
import psycopg2
import mail
import os

alert_eml = mail.gmail['alert_eml']

try:
    connect_str = "dbname=" + os.environ['PG_DATABASE'] + " user=" + \
                  os.environ['PG_USERNAME'] + \
                  " host=" + os.environ['PG_ADDRESS'] + \
                  " password=" + os.environ['PG_PASSWORD']

    # create connection
    conn = psycopg2.connect(connect_str)

    # create cursor
    cursor = conn.cursor()
    check_sql = "SELECT count(*) as cnt FROM sfdc.ss_opportunity where " +\
                "snapshot_date = current_date - interval '1 day'"
    cursor.execute(check_sql)
    rs = cursor.fetchone()
    if rs["cnt"] > 1:
        print("Snapshot exists. Passing.")
        cursor.close()
        pass
    else:
        print("No snapshot found. Creating one.")
        cursor = conn.cursor()
        sql = "INSERT INTO sfdc.ss_opportunity SELECT current_date - interval '1 day', " + \
              "o.* FROM sfdc.opportunity o WHERE isdeleted=FALSE"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    conn.close()

except Exception as e:
    mail.send_message(alert_eml, 'There was an error snapshotting data',e)
