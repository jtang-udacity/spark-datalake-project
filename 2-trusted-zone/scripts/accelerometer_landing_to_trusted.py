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

# Script generated for node Customer Trusted
CustomerTrusted_node1758605666055 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1758605666055")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758605805252 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1758605805252")

# Script generated for node Sanitize accelerometer
Sanitizeaccelerometer_node1758605849109 = Join.apply(frame1=AccelerometerLanding_node1758605805252, frame2=CustomerTrusted_node1758605666055, keys1=["user"], keys2=["email"], transformation_ctx="Sanitizeaccelerometer_node1758605849109")

# Script generated for node Keep accelerometer data
SqlQuery3024 = '''
select user, timestamp, x, y, z from myDataSource
where timestamp >= sharewithresearchasofdate
'''
Keepaccelerometerdata_node1758605957387 = sparkSqlQuery(glueContext, query = SqlQuery3024, mapping = {"myDataSource":Sanitizeaccelerometer_node1758605849109}, transformation_ctx = "Keepaccelerometerdata_node1758605957387")

# Script generated for node Accelerometer trusted
EvaluateDataQuality().process_rows(frame=Keepaccelerometerdata_node1758605957387, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758604730694", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Accelerometertrusted_node1758606023435 = glueContext.getSink(path="s3://jtang97-udacity-project-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusted_node1758606023435")
Accelerometertrusted_node1758606023435.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Accelerometertrusted_node1758606023435.setFormat("json")
Accelerometertrusted_node1758606023435.writeFrame(Keepaccelerometerdata_node1758605957387)
job.commit()