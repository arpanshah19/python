import json
import graphviz

lineage_json_file_path = "F:\\projects\\NCBI\\Project\\ncbi_data_lake_lineage.json"
lineage_chart_name = "lineage_chart"
additional_dependency_file_path = "F:\\projects\\NCBI\\Project\\livevox_dependency.json"

lineage_json_file = open(lineage_json_file_path, "r")
data_lake = json.loads(lineage_json_file.read())
lineage_json_file.close()

# initialize the graph 
f = graphviz.Digraph('finite_state_machine', filename=lineage_chart_name, format='svg')
f.attr(rankdir='LR', size='8,5')
f.attr('node', shape='rectangle')

def add_additional_dependency():
    try:
        additional_dependency = {"stg_livevox.cdr": ["fnd_livevox.cdr","fnd_livevox.agents_summary"]}
        # additional_dependency_file = open(additional_dependency_file_path, 'r')
        # additional_dependency = json.loads(additional_dependency_file.read())
        # additional_dependency_file.close()

        for source_table in additional_dependency:
            target_tables = additional_dependency.get(source_table, [])
            for each_target_table in target_tables:
                f.edge(source_table,each_target_table, label='Table') 
    except Exception as e:
        print(e)

def all_source(withColumns=False):
    for source in data_lake:
        single_source(source, withColumns)

def single_source(source, withColumns=False):
    databases = data_lake[source]
    f.node(source, shape="rarrow", label=source)
    for database in databases:
        f.node(database, shape="circle")
        if len(databases[database].get("dependency", [])) == 0:
            f.edge(source, database, label='') 
        else:
            if( type(databases[database]["dependency"]) == str):
                f.edge(databases[database]["dependency"], database, label='Database Dependency') 
            else:
                for each_dependency in databases[database].get("dependency", []):
                    f.edge(each_dependency, database, label='Database Dependency') 
        
        if len(databases[database].get("tables", [])) > 0:
            for each_table in databases[database]["tables"]:
                table_name = database+"."+each_table
                f.edge(database, table_name, label='Table') 
                if(withColumns):
                    columns = "<TR><TD>Column Name</TD><TD>Column Type</TD></TR>"
                    for column in databases[database]["tables"][each_table]:
                        columns = columns + f'<TR><TD>{column["Name"]}</TD><TD>{column["Type"]}</TD></TR>'

                    f.node(table_name+'_Columns', label=f'<<TABLE>{columns}</TABLE>>')
                    f.edge(table_name, table_name+'_Columns', label='Columns')

single_source(source="livevox", withColumns=True)
add_additional_dependency()
f.view(lineage_chart_name)