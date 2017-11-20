#!/usr/bin/python3
import psycopg2
import sys
from config.dbconfig import pgdb
import mail

alert_eml = mail.gmail['alert_eml']

try:
    connect_str= "dbname='dw_production' user=" + pgdb['user'] + " host=" + pgdb['host']  + \
                 " password=" + pgdb['passwd']

	# create connection
    conn = psycopg2.connect(connect_str)

    # create cursor
    cursor = conn.cursor()

    sql = "INSERT INTO sfdc.ss_opportunity SELECT current_date - interval '1 day', " + \
          "o.* FROM sfdc.opportunity o WHERE isdeleted=FALSE"

    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    mail.send_message(alert_eml,'There was an error snapshotting data',e)
