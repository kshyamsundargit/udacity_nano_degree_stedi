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

# Script generated for node customer_landing
customer_landing_node1749377307544 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1749377307544")

# Script generated for node privacy_filter1
SqlQuery0 = '''
SELECT *
FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
privacy_filter1_node1749380514595 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1749377307544}, transformation_ctx = "privacy_filter1_node1749380514595")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=privacy_filter1_node1749380514595, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749377290351", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1749378829169 = glueContext.getSink(path="s3://kss-spark-project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1749378829169")
customer_trusted_node1749378829169.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
customer_trusted_node1749378829169.setFormat("json")
customer_trusted_node1749378829169.writeFrame(privacy_filter1_node1749380514595)
job.commit()