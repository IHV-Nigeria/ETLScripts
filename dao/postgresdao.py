


def table_creator(table_name, columns):
    column_definitions = []
    for column_name, data_type in columns.items():
        column_definitions.append(f"{column_name} {data_type}")
    columns_sql = ", ".join(column_definitions)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
    return create_table_sql