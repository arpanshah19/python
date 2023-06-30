import boto3
import ast

dependencies = {}
glue_client = boto3.client('glue')

GLUE_JOB_BUCKET = "aws-glue-assets-867070907590-us-east-1"
def get_glue_job_path(job_name):
    response = glue_client.get_job(JobName=job_name)
    print("response", response)
    return response['Job']['Command']['ScriptLocation']


def get_job_code(job_key):
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(GLUE_JOB_BUCKET)
    print("job_key", job_key)
    code_obj = s3_bucket.Object(job_key)
    code = code_obj.get()['Body'].read().decode('utf-8')
    print("code", code)
    return code

def get_job_name(job_key):
    keys = job_key.split("/")
    return keys[len(keys)-1]

def extract_source_target_tables(code):
    source_tables = []
    target_tables = []

    # Parse the Python code
    parsed = ast.parse(code)

    # Traverse the abstract syntax tree
    for node in ast.walk(parsed):
        if isinstance(node, ast.Call):
            function_name = getattr(node.func, 'id', '')
            if function_name == 'read' or function_name == 'load':
                table_name = node.args[0].s
                source_tables.append(table_name)
            elif function_name == 'write' or function_name == 'save':
                table_name = node.args[0].s
                target_tables.append(table_name)

    return source_tables, target_tables

def main():
    jobs  = glue_client.list_jobs()
    print("jobs", jobs["JobNames"])
    
    for each_job in jobs:
        job_path = get_glue_job_path(each_job)
        print("job path", job_path)

        job_key = job_path.split(GLUE_JOB_BUCKET+"/")[1]
        job_code = get_job_code(job_key)
        
        job_name = get_job_name(job_key)
        source_tables, target_tables = extract_source_target_tables(job_code)
        print("Source Tables:", source_tables)
        print("Target Tables:", target_tables)
        dependencies[each_job] = { 
            "source_tables": source_tables,
            "target_tables": target_tables
        }


main()
print(dependencies)




#===========read spark plan=========
# from pyspark.sql import SparkSession

# # Create a SparkSession
# spark = SparkSession.builder.getOrCreate()

# # Execute the PySpark code from another file
# exec(open('your_code_file.py').read())

# # Extract source and target tables from the execution plan
# def extract_tables(plan):
#     source_tables = []
#     target_tables = []

#     def extract_tables_recursive(plan_node):
#         if hasattr(plan_node, 'tableIdentifier'):
#             table_name = plan_node.tableIdentifier().table()
#             if table_name not in source_tables and table_name not in target_tables:
#                 source_tables.append(table_name)
#         else:
#             for child in plan_node.children():
#                 extract_tables_recursive(child)

    # extract_tables_recursive(plan)

    # return source_tables, target_tables

# Extract source and target tables from the execution plan
# source_tables, target_tables = extract_tables(spark._jsparkSession.sessionState().executePlan(df._jdf.queryExecution().logical()))

# # Print the source and target table lists
# print("Source Tables:")
# for source_table in source_tables:
#     print(source_table)

# print("\nTarget Tables:")
# for target_table in target_tables:
#     print(target_table)
