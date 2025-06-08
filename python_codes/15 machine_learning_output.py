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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1749390425239 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1749390425239")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1749390480903 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/step-trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1749390480903")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
at.user
,at.x
,at.y
,at.z
,stt.sensorreadingtime
,stt.serialnumber
,stt.distancefromobject
FROM 
step_trainer_trusted stt
INNER JOIN 
accelerometer_trusted at
ON 
stt.sensorreadingtime = at.timestamp;
'''
SQLQuery_node1749390507271 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_trusted_node1749390425239, "step_trainer_trusted":step_trainer_trusted_node1749390480903}, transformation_ctx = "SQLQuery_node1749390507271")

# Script generated for node machine_learning_output
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749390507271, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749390371511", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_output_node1749390845894 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1749390507271, connection_type="s3", format="json", connection_options={"path": "s3://kss-spark-project/machine_learning/", "partitionKeys": []}, transformation_ctx="machine_learning_output_node1749390845894")

job.commit()