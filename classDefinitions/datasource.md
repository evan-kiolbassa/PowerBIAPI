# PowerBIDataSource Class

The PowerBIDataSource class is a Python class for creating and manipulating Power BI data sources using the Power BI API. This class has four methods, which allow you to create a new table in a dataset, append rows to an existing table, update rows in an existing table, and delete rows from an existing table.

## Requirements
- Python 3.x
- The `requests` library


## Usage
The PowerBIDataSource class can be used to create, update, and delete tables in a Power BI dataset. To use this class, follow these steps:

Import the PowerBIDataSource class from the power_bi_data_source.py module:

```python
from power_bi_data_source import PowerBIDataSource
Create an instance of the PowerBIDataSource class with your Azure Active Directory application's client_id, client_secret, tenant_id, and api_version (if different from the default 'v1.0'):

pbi = PowerBIDataSource(client_id='your_client_id', client_secret='your_client_secret', tenant_id='your_tenant_id', api_version='v1.0')
```

Note: You can find your Azure Active Directory application's client_id and client_secret in the Azure portal. You can find your tenant_id in the Azure Active Directory blade of the Azure portal.

Call the connect method to authenticate with the Power BI API and set the headers for API requests:

```python
pbi.connect()

# Use the create_table method to create a new table in a dataset:

dataset_id = 'your_dataset_id'
table_name = 'your_table_name'
table_definition = [
    {'name': 'column1', 'data_type': 'string'},
    {'name': 'column2', 'data_type': 'int'},
    {'name': 'column3', 'data_type': 'datetime'},
    # ...
]
pbi.create_table(dataset_id=dataset_id, table_name=table_name, table_definition=table_definition)
```
Note: The table_definition parameter should be a list of dictionaries that define the columns of the table and their data types.

Use the append_rows method to append rows to an existing table in a dataset:


```python
dataset_id = 'your_dataset_id'
table_name = 'your_table_name'
rows = [
    {'column1': 'value1', 'column2': 123, 'column3': '2022-02-20T00:00:00Z'},
    {'column1': 'value2', 'column2': 456, 'column3': '2022-02-21T00:00:00Z'},
    # ...
]
pbi.append_rows(dataset_id=dataset_id, table_name=table_name, rows=rows)
```
Note: The rows parameter should be a list of dictionaries representing the rows to be appended. The keys of each dictionary should be the column names, and the values should be the corresponding values.

Use the update_rows method to update rows in an existing table in a dataset:

```python
dataset_id = 'your_dataset_id'
table_name = 'your_table_name'
update_query = "update your_table_name set column1 = 'new_value' where column2 = 123"
pbi.update_rows(dataset_id=dataset_id, table_name=table_name, update_query=update_query)
```
Note: The update_query parameter should be a string representing the update query to be executed.

Use the delete_rows method to delete rows from an existing table in a dataset:

```python
dataset_id = 'your_dataset_id'
table_name = 'your_table_name'
```
Use the delete_rows method to delete rows from an existing table in a dataset:

```python
# This will delete all rows in the table where the value in the specified column matches ValueToDelete.
delete_query = """
    let
        Source = #"Table Name",
        Filter = Table.SelectRows(Source, each [Column Name] = ValueToDelete),
        Delete = Table.RemoveRows(Source, Filter[Row])
    in
        Delete
"""

pbi.delete_rows(dataset_id, table_name, delete_query)
```
## Conclusion
The PowerBIDataSource class provides a simple and flexible way to create and manipulate data sources in Power BI using the Power BI API. With this class, you can create tables, append and update rows, and delete rows from an existing table. These methods can be used to automate the data preparation and cleansing process, making it easy to keep your data up to date in Power BI.