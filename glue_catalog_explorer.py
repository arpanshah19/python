import boto3
import json

def retrieve_metadata(database_name):
    glue_client = boto3.client('glue', region_name='us-east-1')
    tables = glue_client.get_tables(DatabaseName=database_name)['TableList']
    metadata = {}
    print("tables", tables)
    for table in tables:
        table_name = table['Name']
        columns = table['StorageDescriptor']['Columns']
        metadata[table_name] = columns

    return metadata

data_lake_sources = {
        "matomo": {"stg_matomo":{},
                    "fnd_matomo": {"dependency": ["stg_matomo"]}
                  },
        "livevox": {"stg_livevox":{}, 
                    "fnd_livevox":{"dependency": ["stg_livevox"]}
                   },
        "debt_master": {"stg_debt_master":{}, 
                        "fnd_debt_master":{"dependency": ["stg_debt_master"]}
                    },
        "debt_wizard": {"stg_dms":{}, 
                        "fnd_dms":{"dependency": ["stg_dms"]}
                    }
}

for source in data_lake_sources:
    databases = data_lake_sources[source]
    for database in databases:
        metadata = retrieve_metadata(database)
        databases[database]["tables"] = metadata
        print(databases, metadata)
print("======================Final===================")
client = boto3.client('s3')
client.put_object(Body=json.dump(data_lake_sources), Bucket='s3-lnd', Key=f'data_lake_lineage.json')
print(data_lake_sources)
