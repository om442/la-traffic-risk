from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import Row
import json
import pandas as pd
from pyspark.sql import SQLContext
from transform_data_copy import getParameters


def main():
    # Task configuration.
    topic = "traffic_data"
    brokerAddresses = "localhost:9092"
    batchTime = 10

    # Creating stream.
    spark = SparkSession.builder.appName("TrafficData").getOrCreate()
    sc = spark.sparkContext
    sql = SQLContext(sc)
    ssc = StreamingContext(sc, batchTime)
    # Getting data from kafka stream.
    kvs = KafkaUtils.createDirectStream(
        ssc, [topic], {"metadata.broker.list": brokerAddresses})

    parsed = kvs.map(lambda v: json.loads(v[1]))

    def pp(rdd):
        l = rdd.collect()
        df = 0
        df2 = pd.DataFrame()
        c = 0
        for r in l:

            df2 = pd.DataFrame()
            for i in r:
                # Modify the latitute and longitute location for geo pandas in the format you want
                i['location_1'] = '('+str(i['location_1']['latitude']) + \
                    ', '+str(i['location_1']['longitude'])+')'

                # Remove uneccessary columns in dataframe
                if ':@computed_region_qz3q_ghft' in i:
                    del i[':@computed_region_qz3q_ghft']
                if ':@computed_region_k96s_3jcv' in i:
                    del i[':@computed_region_k96s_3jcv']
                if ':@computed_region_tatf_ua23' in i:
                    del i[':@computed_region_tatf_ua23']
                if ':@computed_region_ur2y_g4cx' in i:
                    del i[':@computed_region_ur2y_g4cx']
                if ':@computed_region_kqwf_mjcx' in i:
                    del i[':@computed_region_kqwf_mjcx']
                if ':@computed_region_2dna_qi2s' in i:
                    del i[':@computed_region_2dna_qi2s']

                df1 = pd.DataFrame(i, index=[c])

                df2 = df2.append(df1, ignore_index=True)
                c += 1
            break

        # Check if dataframe is empty before appending on to csv file in S3
        if not(df2.empty):

            # Convert datetime to the correct format
            df2['date_rptd'] = pd.to_datetime(
                df2['date_rptd']).dt.strftime('%m/%d/%Y')
            df2['date_occ'] = pd.to_datetime(
                df2['date_occ']).dt.strftime('%m/%d/%Y')

            AWS_ACCESS_KEY_ID = "Enter access key id here"
            AWS_SECRET_ACCESS_KEY = "Enter secret access key here"

            # Converting dataframe to a .csv file and appending it on to a s3 bucket .csv file
            df2.to_csv(
                f"s3://551projectcsv/load/New_Traffic_Collision_Data_from_2010_to_Present.csv",
                encoding="utf-8",
                mode='a',
                index=False,
                header=False,
                storage_options={
                    "key": AWS_ACCESS_KEY_ID,
                    "secret": AWS_SECRET_ACCESS_KEY
                },
            )

    parsed.foreachRDD(lambda x: pp(x))

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
