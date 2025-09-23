import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node step trainer landing
steptrainerlanding_node1758625269478 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1758625269478")

# Script generated for node customer curated
customercurated_node1758625294719 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtang97-udacity-project-bucket/customer/curated/"], "recurse": True}, transformation_ctx="customercurated_node1758625294719")

# Script generated for node Renamed keys for Clean step trainer data
RenamedkeysforCleansteptrainerdata_node1758625375643 = ApplyMapping.apply(frame=customercurated_node1758625294719, mappings=[("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("phone", "string", "right_phone", "string"), ("birthday", "string", "right_birthday", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforCleansteptrainerdata_node1758625375643")

# Script generated for node Clean step trainer data
steptrainerlanding_node1758625269478DF = steptrainerlanding_node1758625269478.toDF()
RenamedkeysforCleansteptrainerdata_node1758625375643DF = RenamedkeysforCleansteptrainerdata_node1758625375643.toDF()
Cleansteptrainerdata_node1758625334514 = DynamicFrame.fromDF(steptrainerlanding_node1758625269478DF.join(RenamedkeysforCleansteptrainerdata_node1758625375643DF, (steptrainerlanding_node1758625269478DF['serialnumber'] == RenamedkeysforCleansteptrainerdata_node1758625375643DF['right_serialnumber']), "leftsemi"), glueContext, "Cleansteptrainerdata_node1758625334514")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=Cleansteptrainerdata_node1758625334514, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758623717176", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1758625548518 = glueContext.getSink(path="s3://jtang97-udacity-project-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1758625548518")
steptrainertrusted_node1758625548518.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1758625548518.setFormat("json")
steptrainertrusted_node1758625548518.writeFrame(Cleansteptrainerdata_node1758625334514)
job.commit()