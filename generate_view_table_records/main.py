from typing import Any, Mapping, Union
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
import argparse

def generate_view_table_records(project : str, dataset_id : str, table_id : str): 
    table_schema = get_table_schema(project, dataset_id, table_id)
    sql = get_sql_by_table_schema(table_schema, project, dataset_id, table_id)
    print(f"Comand: {sql}")

def get_table_schema(project : str, dataset_id : str, table_id : str) -> Union[SchemaField, Mapping[str, Any] ]: 
    biquery_client = bigquery.Client()
    dataset_ref = biquery_client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = biquery_client.get_table(table_ref)  # API Request

    # View table properties
    print(f"Table Schema: {table.schema}")
    return table.schema

def get_sql_by_table_schema(table_schema : SchemaField, project : str, dataset_id : str, table_id : str) -> str:
    sql_projection = f"SELECT {table_id}.*"
    sql_project_exception = ""
    sql_join = f"FROM `{project}.{dataset_id}.{table_id}` AS {table_id}"
    list_join = []
    list_fields = []

    for schema in table_schema:
        if schema.field_type != 'RECORD':
            continue
        
        if len(sql_project_exception) == 0:
            sql_project_exception = f"EXCEPT({schema.name}"
        else:
            sql_project_exception = f"{sql_project_exception},{schema.name}"

        list_join_record, list_fields_record = get_fields_and_join(schema,'')
        list_join.extend(list_join_record)
        list_fields.extend(list_fields_record)

    if len(sql_project_exception) > 0:
        sql_project_exception = f"{sql_project_exception})"

    sql_projection = f"{sql_projection} {sql_project_exception}"

    for field in list_fields:
        sql_projection = f"{sql_projection} \n,{field}"

    for join in list_join:
        alias_name = join.replace('.', '')
        join_field = ""

        path_parts = join.split('.')
        for index in range(len(path_parts) - 1):
            join_field = f"{join_field}{path_parts[index]}" 

        if type(path_parts) == list:
            join_field = f"{join_field}.{path_parts[-1]}"

        if join_field.startswith("."):
            join_field = f"{table_id}{join_field}"

        sql_join = f"{sql_join} \nLEFT JOIN UNNEST({join_field}) as {alias_name} on 1 = 1"

    sql = f"{sql_projection}\n{sql_join}"
    return sql

def get_fields_and_join(schema : SchemaField, path : str):
    new_path = f"{path}{schema.name}"
    list_join = [new_path]
    list_fields = []

    for field in schema.fields:
        if field.field_type == 'RECORD':
           list_join_record, list_fields_record = get_fields_and_join(field,new_path + '.')
           list_join.extend(list_join_record)
           list_fields.extend(list_fields_record)
        else:
            element = f"{new_path.replace('.', '')}.{field.name} as {new_path.replace('.', '')}{field.name}"
            list_fields.append(element)

    return list_join, list_fields

if __name__ == '__main__':
     # Application Arguments
    parser = argparse.ArgumentParser(description='Generate View Table Records.')
    parser.add_argument('--project', type=str, required=True, help='GCP Project')
    parser.add_argument('--dataset_id', type=str, required=True, help='Dataset id')
    parser.add_argument('--table_id', type=str, required=True, help='Table Id')
    args = parser.parse_args()

    generate_view_table_records(args.project, args.dataset_id, args.table_id)