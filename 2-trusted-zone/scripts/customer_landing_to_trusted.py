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

# Script generated for node Customer Landing
CustomerLanding_node1758604184827 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/customer/landing/run-1758510031866-part-r-00000"], "recurse": True}, transformation_ctx="CustomerLanding_node1758604184827")

# Script generated for node Filter for customer consent
SqlQuery2620 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
Filterforcustomerconsent_node1758604319349 = sparkSqlQuery(glueContext, query = SqlQuery2620, mapping = {"myDataSource":CustomerLanding_node1758604184827}, transformation_ctx = "Filterforcustomerconsent_node1758604319349")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=Filterforcustomerconsent_node1758604319349, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758604091225", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1758604423176 = glueContext.getSink(path="s3://jtang97-udacity-project-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1758604423176")
CustomerTrusted_node1758604423176.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1758604423176.setFormat("json")
CustomerTrusted_node1758604423176.writeFrame(Filterforcustomerconsent_node1758604319349)
job.commit()