from config import config
from utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, regexp_replace

def define_udfs():
    return {
        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_postion, StringType()),
        'extract_classcode_udf': udf(extract_classcode, StringType()),
        'extract_salary_udf': udf(extract_salary, StructType([
            StructField('salary_start', DoubleType()),
            StructField('salary_end', DoubleType()),
        ])),
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_end_date_udf': udf(extract_end_date, DateType()),
        'extract_requirements_udf': udf(extract_requirements, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_selection_udf': udf(extract_selection, StringType()),
        'extract_experience_required_udf': udf(extract_experience_required, StringType()),
        'extract_education_years_udf': udf(extract_education_years, StringType()),
        'extract_application_location_udf': udf(extract_application_location, StringType()),
    }

if __name__ == "__main__":
    spark = (SparkSession.builder.appName('???')
            .config('spark.jars.packages',
                    'org.apache.hadoop:hadoop-aws:3.3.1,'
                    'com.amazonaws:aws-java-sdk:1.11.469')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.access.key', config.get('AWS_ACCESS_KEY'))
            .config('spark.hadoop.fs.s3a.secret.key', config.get('AWS_SECRET_KEY'))
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .getOrCreate())
    
    # change to docker image file path
    text_input_dir = "file:///home/flowstate/aws-spark-unstructured-data-pipeline/data"

    data_schema = StructType([
        StructField('file_name', StringType()),
        StructField('position', StringType()),
        StructField('classcode', StringType()),
        StructField('salary_start', DoubleType()),
        StructField('salary_end', DoubleType()),
        StructField('start_date', DateType()),
        StructField('end_date', DateType()),
        StructField('requirements', StringType()),
        StructField('notes', StringType()),
        StructField('duties', StringType()),
        StructField('selection', StringType()),
        StructField('experience_required', StringType()),
        StructField('education_years', StringType()),
        StructField('school_type', StringType()),
        StructField('job_type', StringType()),
        StructField('application_location', StringType()),
    ])

    udfs = define_udfs()

    job_bulletins_df = (spark
                        .readStream
                        .format("text")
                        .option("wholetext", "true")
                        .load(text_input_dir))
    
    job_bulletins_df = job_bulletins_df.withColumn('file_name', 
                                                   regexp_replace(udfs['extract_file_name_udf']('value'), r'\r', ' '))
    
    job_bulletins_df = job_bulletins_df.withColumn('value', 
                                                   regexp_replace('value', r'\n', ' '))
    
    job_bulletins_df = job_bulletins_df.withColumn('position', 
                                                   regexp_replace(udfs['extract_position_udf']('value'), r'\r', ' '))
    
    job_bulletins_df = job_bulletins_df.withColumn('classcode', 
                                                   udfs['extract_classcode_udf']('value'))

    job_bulletins_df = job_bulletins_df.withColumn('salary_start', 
                                                   udfs['extract_salary_udf']('value').getField('salary_start'))
    
    job_bulletins_df = job_bulletins_df.withColumn('salary_end', 
                                                   udfs['extract_salary_udf']('value').getField('salary_end'))
    
    job_bulletins_df = job_bulletins_df.withColumn('start_date', 
                                                   udfs['extract_start_date_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('end_date', 
                                                   udfs['extract_end_date_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('requirements', 
                                                   udfs['extract_requirements_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('notes', 
                                                   udfs['extract_notes_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('duties', 
                                                   udfs['extract_duties_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('selection', 
                                                   udfs['extract_selection_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('experience_required', 
                                                   udfs['extract_experience_required_udf']('value'))
    
    job_bulletins_df = job_bulletins_df.withColumn('education_years', 
                                                   udfs['extract_education_years_udf']('value'))

    job_bulletins_df = job_bulletins_df.withColumn('application_location', 
                                                   udfs['extract_application_location_udf']('value'))    
    
    
    

    query = (job_bulletins_df
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", "false")
             .start())
    
    query.awaitTermination()