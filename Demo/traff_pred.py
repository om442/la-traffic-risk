from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col, max


def traffic_pred():
    spark = SparkSession.builder.appName(
        "traffic").enableHiveSupport().getOrCreate()

    jdbcURL = "Enter jdbc url and auth credentials here"
    # Enter your temporary S3 directory that you wish to use here
    tempS3Dir = "s3://551projectcsv/load/"

    # Read data from a table
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbcURL) \
        .option("dbtable", "traffic_monthly") \
        .option("tempdir", tempS3Dir) \
        .load()

    data_to_2010 = df.filter(df.year <= 2010)
    data_to_2011 = df.filter(df.date_occ == df.agg(
        max(df.date_occ)).collect()[0][0])

    label_stringIdx = StringIndexer(inputCol="risk_label", outputCol="label")
    encoder = OneHotEncoder(dropLast=False,
                            inputCols=["area", "year", 'month'],
                            outputCols=["area_vec", "year_vec", 'month_vec'])
    assembler = VectorAssembler(inputCols=["area_vec", "year_vec", 'month_vec'],
                                outputCol="features")

    pipeline = Pipeline(stages=[encoder, label_stringIdx, assembler])

    # Fit the pipeline to training documents.
    pipelineFit = pipeline.fit(data_to_2010)
    dataset = pipelineFit.transform(data_to_2010)

    data_to_2010 = dataset.filter(dataset.year <= 2010)
    data_to_2011 = dataset.filter(
        dataset.date_occ == dataset.agg(max(dataset.date_occ)).collect()[0][0])

    (trainingData, testData) = data_to_2010.randomSplit([0.7, 0.3], seed=100)
    print("Training Dataset Count: " + str(trainingData.count()))
    print("Test Dataset Count: " + str(testData.count()))

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0)
    lrModel = lr.fit(trainingData)
    predictions = lrModel.transform(data_to_2011)

    # final_preds = predictions.select("prediction").collect()
    # predictions['risk_label'] = final_preds

    return predictions
