import os
import sys
import snowflake.connector
import httpx

def get_snowflake_columns(
    snowflake_connection: snowflake.connector,
    sfdc_object_name: str,
    table_prefix: str = "SFDC_",
    table_postfix: str = "_OBJECT",
    snowflake_schema: str = os.environ.get("SNOWFLAKE_SCHEMA")
):

    snowflake_column_names = []
    cs = snowflake_connection.cursor()

    try:
        cs.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_prefix.upper()}{sfdc_object_name.upper()}{table_postfix.upper()}' 
            AND table_schema = '{snowflake_schema.upper()}'
            ORDER BY ordinal_position;
            """
        )
        if cs.rowcount > 0:
            # logger.info(f"There are {cs.rowcount} columns in Snowflake for 'SFDC_{self.query_object}_{table_postfix}'")
            for column_name in cs:
                snowflake_column_names.append(column_name[0].upper())
        else:
            print(
                f"No columns returned for '{table_prefix.upper()}{sfdc_object_name.upper()}{table_postfix.upper()}' on  schema '{snowflake_schema.upper()}'",
                file=sys.stderr
            )
    finally:
        cs.close()

    return snowflake_column_names


def get_salesforce_fields(client: httpx.Client, field_name: str):

    try:
        response = client.get(url="")