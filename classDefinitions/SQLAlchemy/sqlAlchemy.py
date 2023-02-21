from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select
from sqlalchemy import text, select, Table, MetaData, case, func, over

class MSSQLDatabase:
    def __init__(self, connection_string, database_name, schema_name, table_name):
        self.engine = create_engine(connection_string)
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name

    def create_table(self, columns):
        meta = MetaData()
        table = Table(self.table_name, meta, schema=self.schema_name)
        for column in columns:
            table.append_column(Column(column['name'], eval(column['type'])))
        table.create(self.engine)
        print(f"Table {self.database_name}.{self.schema_name}.{self.table_name} created.")

    def insert_dask_dataframe(self, df, if_exists='fail'):
        meta = MetaData()
        with self.engine.connect() as conn:
            meta.reflect(bind=conn, schema=self.schema_name, views=True)
            table = Table(self.table_name, meta, schema=self.schema_name, autoload=True, autoload_with=conn)
            df.to_sql(name=self.table_name, con=conn, schema=self.schema_name, if_exists=if_exists, index=False)

    def append_table(self, rows):
        meta = MetaData()
        with self.engine.connect() as conn:
            meta.reflect(bind=conn, schema=self.schema_name, views=True)
            table = Table(self.table_name, meta, schema=self.schema_name, autoload=True, autoload_with=conn)
            for row in rows:
                insert = table.insert().values(row)
                conn.execute(insert)
        print(f"{len(rows)} rows appended to table {self.database_name}.{self.schema_name}.{self.table_name}.")

    def update_table(self, update_query):
        with self.engine.connect() as conn:
            conn.execute(text(update_query))
        print(f"Table {self.database_name}.{self.schema_name}.{self.table_name} updated.")

    def delete_table(self, delete_query):
        with self.engine.connect() as conn:
            conn.execute(text(delete_query))
        print(f"Table {self.database_name}.{self.schema_name}.{self.table_name} deleted.")

    def select_table(self, select_cols, *args, ctes=None, **kwargs):
        meta = MetaData()
        with self.engine.connect() as conn:
            meta.reflect(bind=conn, schema=self.schema_name, views=True)
            table = Table(self.table_name, meta, schema=self.schema_name, autoload=True, autoload_with=conn)

            if ctes is not None:
                cte_query = ""
                for cte in ctes:
                    cte_name = cte['name']
                    cte_cols = cte['columns']
                    cte_query += f"{cte_name} AS (SELECT {', '.join(cte_cols)} {cte['query']}) "
                query = text(cte_query + f"SELECT {', '.join(select_cols)} FROM {self.schema_name}.{self.table_name}")
            else:
                query = select([table.c[col] for col in select_cols])

            for arg in args:
                if arg['type'] == 'where':
                    col = arg['column']
                    op = arg['operator']
                    val = arg['value']
                    query = query.where(table.c[col] == val)
                elif arg['type'] == 'group_by':
                    cols = arg['columns']
                    query = query.group_by(*[table.c[col] for col in cols])
                elif arg['type'] == 'having':
                    col = arg['column']
                    op = arg['operator']
                    val = arg['value']
                    query = query.having(table.c[col] == val)
                elif arg['type'] == 'case_if':
                    col = arg['column']
                    conditions = []
                    for condition in arg['conditions']:
                        conditions.append((table.c[condition['column']] == condition['value'], condition['result']))
                    default = arg.get('default', None)
                    case_expression = case(conditions, else_=default)
                    query = query.add_columns(case_expression.label(col))
                elif arg['type'] == 'count':
                    col = arg['column']
                    query = query.add_columns(func.count(table.c[col]).label(col))
                elif arg['type'] == 'window_function':
                    func_name = arg['function']
                    col = arg['column']
                    partition_by_cols = arg.get('partition_by', None)
                    order_by_cols = arg.get('order_by', None)
                    if partition_by_cols and order_by_cols:
                        window = over(partition_by=[table.c[col] for col in partition_by_cols], order_by=[table.c[col] for col in order_by_cols])
                    elif order_by_cols:
                        window = over(order_by=[table.c[col] for col in order_by_cols])
                    else:
                        window = over()
                    query = query.add_columns(getattr(func, func_name)(table.c[col]).over(window).label(col))

            result = conn.execute(query).fetchall()
        return result