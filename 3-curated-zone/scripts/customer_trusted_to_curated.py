import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758623936662 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1758623936662")

# Script generated for node Customer Trusted
CustomerTrusted_node1758623880591 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1758623880591")

# Script generated for node Customer Data Cleaning
CustomerDataCleaning_node1758623956833 = Join.apply(frame1=AccelerometerTrusted_node1758623936662, frame2=CustomerTrusted_node1758623880591, keys1=["user"], keys2=["email"], transformation_ctx="CustomerDataCleaning_node1758623956833")

# Script generated for node SQL Query
SqlQuery2794 = '''
select 
distinct
customername,
email,
phone,
birthday,
serialnumber,
registrationdate,
lastupdatedate,
sharewithresearchasofdate,
sharewithpublicasofdate,
sharewithfriendsasofdate
from myDataSource
'''
SQLQuery_node1758624007694 = sparkSqlQuery(glueContext, query = SqlQuery2794, mapping = {"myDataSource":CustomerDataCleaning_node1758623956833}, transformation_ctx = "SQLQuery_node1758624007694")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758624007694, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758623717176", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1758624096058 = glueContext.getSink(path="s3://jtang97-udacity-project-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1758624096058")
CustomerCurated_node1758624096058.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1758624096058.setFormat("json")
CustomerCurated_node1758624096058.writeFrame(SQLQuery_node1758624007694)
job.commit()