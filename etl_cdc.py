from pyspark.sql import SparkSession
from pyspark.sql.functions import col, json_tuple, from_json
from pyspark.sql.types import StringType, StructField, StructType

DATABASE_NAME='{database_name}'
TABLE_NAME='{table_name}'
LOCATION = '{warehouse.path.dir}/{database_name}'
TOPIC = '{topic_name}'
BOOTSTRAP_SERVERS = '{kafka=bootstrap}'

employee_schema = StructType([
    StructField('Employee_Name', StringType(), True),
    StructField('PositionID', StringType(), True),
    StructField('Position', StringType(), True),
    StructField('PerformanceScore', StringType(), True),
    StructField('Absences', StringType(), True)
])

queryCreateTable=f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} (
        Employee_Name STRING,
        PositionID STRING,
        Position STRING,
        PerformanceScore STRING,
        Absences STRING)
        USING DELTA
        LOCATION {LOCATION}"""

def insertToDelta(df, mode='append'):
    df \
        .select('Employee_Name', 'PositionID', 'Position', 'PerformanceScore', 'Absences') \
        .write \
        .format('delta') \
        .mode(mode) \
        .saveAsTable(f'{DATABASE_NAME}.{TABLE_NAME}')

def updelToDelta(spark, row):
    list_query = row['query'].split('employee')
    list_query.insert(1, f'{DATABASE_NAME}.{TABLE_NAME}') ## Replace table name <table-name> --> <database-name>.<table-name>
    newQuery = "".join(list_query)
    spark.sql(newQuery)

def toDelta(spark, df):
    ## Handle error while spark streams first run
    if df.count() > 0:
        ## Filtering dataframe by operation
        read = df.filter(col('op') == 'r')
        create = df.filter(col('op') == 'c')
        update = df.filter(col('op') == 'u')
        delete = df.filter(col('op') == 'd')

        read_df = getPayload(read, employee_schema)
        create_df = getPayload(create, employee_schema)
        update_df = getPayload(update, employee_schema)
        delete_df = getPayload(delete, employee_schema, columns='before')

        if not read_df.rdd.isEmpty():
            insertToDelta(read_df, mode='overwrite')

        if not create_df.rdd.isEmpty():
            insertToDelta(create_df)

        if not update_df.rdd.isEmpty():
            for row in update_df.select('query').collect():
                updelToDelta(spark, row)

        if not delete_df.rdd.isEmpty():
            for row in delete_df.select('query').collect():
                updelToDelta(spark, row)

def getPayload(df, schema, columns='after'):
    return df \
        .withColumn('data', from_json(col(f'{columns}'), schema)) \
        .select('op', 'data.*', 'query')

def main():
    spark = SparkSession \
        .builder \
        .appName('ETL application') \
        .enableHiveSupport() \
        .getOrCreate()
    
    schema_change = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
        .option('subscribe', TOPIC) \
        .option('startingOffsets', 'earliest') \
        .load() \
        .selectExpr('CAST(value AS STRING)')

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    payload_df = schema_change \
        .withColumn('payload', json_tuple('value', 'payload')) \
        .withColumn('op', json_tuple('payload', 'op')) \
        .withColumn('before', json_tuple('payload', 'before')) \
        .withColumn('after', json_tuple('payload', 'after')) \
        .withColumn('source', json_tuple('payload', 'source')) \
        .withColumn('query', json_tuple('source', 'query')) \
        .select('op', 'before', 'after', 'query')

    
    payload_df \
        .writeStream \
        .foreachBatch(lambda df, id: toDelta(spark, df)) \
        .start()

    schema_change \
        .writeStream \
        .format('console') \
        .outputMode('append') \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
