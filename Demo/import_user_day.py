import redshift_connector
import numpy
import pandas


def importDay():
    conn = redshift_connector.connect(
        host='redshift host cluster detail here',
        port=5439,
        database='put your database name here',
        user='put your user name',
        password='put your password here'
    )

    cursor: redshift_connector.Cursor = conn.cursor()

    query = "DROP TABLE IF EXISTS traffic_daily_uploaded;"
    cursor.execute(query)

    # Create table with right columns and data types
    query = '''
    CREATE TABLE traffic_daily_uploaded
    (
      date_occ        DATE,
      year            INTEGER,
      month           INTEGER,
      day             INTEGER,
      area_name       TEXT,
      area            INTEGER,
      counts          INT8,
      count_diff      INT8,
      risk_weighting  FLOAT,
      risk_label      TEXT,
      constraint traffic_daily_uploaded_pk
          primary key (date_occ, area)
    );
    '''
    cursor.execute(query)

    query = '''
    COPY traffic_daily_uploaded
    FROM 's3://551projectcsv/load/User_Uploaded_Day_Traffic_Collision_Data_from_2010_to_Present.csv'
    credentials 'put your aws credentials here'
    CSV
    IGNOREHEADER 1;
    '''
    cursor.execute(query)

    conn.commit()
    conn.close()
