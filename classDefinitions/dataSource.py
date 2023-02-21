import requests

class PowerBIDataSource:
    """
    A class for creating and manipulating Power BI data sources using the Power BI API.
    """

    def __init__(self, client_id, client_secret, tenant_id, api_version='v1.0'):
        """
        Constructor for the PowerBIDataSource class.

        Parameters:
            client_id (str): The client ID for the Azure Active Directory application.
            client_secret (str): The client secret for the Azure Active Directory application.
            tenant_id (str): The ID of the Azure Active Directory tenant.
            api_version (str): The version of the Power BI API to use (default is 'v1.0').
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.api_version = api_version
        self.access_token = None
        self.headers = None

    def connect(self):
        """
        Creates an access token and sets the headers for API requests.
        """
        auth_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
        auth_data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': 'https://analysis.windows.net/powerbi/api'
        }
        auth_resp = requests.post(auth_url, data=auth_data)
        if auth_resp.status_code == 200:
            self.access_token = auth_resp.json()['access_token']
            self.headers = {'Authorization': f'Bearer {self.access_token}'}
        else:
            raise ValueError(f"""Could not authenticate with Power BI API. Error {
                auth_resp.status_code
                }: {
                auth_resp.text
                }""")

    def create_table(self, dataset_id, table_name, table_definition):
        """
        Creates a new table in the specified dataset.

        Parameters:
            dataset_id (str): The ID of the dataset to create the table in.
            table_name (str): The name of the table to be created.
            table_definition (list of dict): A list of dictionaries that define the columns of the table and their data types.

        Raises:
            ValueError: If the specified dataset does not exist in the workspace.

        Returns:
            None
        """
        self.connect()

        # Check if the specified dataset exists in the workspace
        datasets_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/datasets'
        datasets_resp = requests.get(datasets_url, headers=self.headers)
        if datasets_resp.status_code != 200:
            raise ValueError(f"Could not retrieve datasets from Power BI API. Error {datasets_resp.status_code}: {datasets_resp.text}")
        datasets = datasets_resp.json()['value']
        if not any(d['id'] == dataset_id for d in datasets):
            raise ValueError(f"Dataset {dataset_id} does not exist in the workspace.")

        # Create the table
        tables_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/tables'
        table_data = {
            'name': table_name,
            'columns': [
                {
                    'name': col['name'],
                    'dataType': col['data_type']
                } for col in table_definition
            ]
        }
        table_resp = requests.post(tables_url, headers=self.headers, json=table_data)
        if table_resp.status_code != 201:
            raise ValueError(f"Could not create table. Error {table_resp.status_code}: {table_resp.text}")

    def append_rows(self, dataset_id, table_name, rows):
        """
        Appends rows to an existing table in the specified dataset.

        Parameters:
            dataset_id (str): The ID of the dataset containing the table to append rows to.
            table_name (str): The name of the table to append rows to.
            rows (list of dict): A list of dictionaries representing the rows to be appended.

        Raises:
            ValueError: If the specified dataset or table does not exist in the workspace, or if the API returns an error.

        Returns:
            None
        """
        self.connect()

        # Check if the specified dataset and table exist in the workspace
        tables_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/tables'
        tables_resp = requests.get(tables_url, headers=self.headers)
        if tables_resp.status_code != 200:
            raise ValueError(f"Could not retrieve tables from Power BI API. Error {tables_resp.status_code}: {tables_resp.text}")
        tables = tables_resp.json()['value']
        table = next((t for t in tables if t['name'] == table_name), None)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist in dataset {dataset_id}.")

        # Append the rows to the table
        rows_url = f'''https://api.powerbi.com/{
            self.api_version
            }/myorg/groups/{
                dataset_id
                }/tables/{table["id"]}/rows'''
        rows_resp = requests.post(rows_url, headers=self.headers, json={'rows': rows})
        if rows_resp.status_code != 200 and rows_resp.status_code != 201:
            raise ValueError(
                f"""Could not append rows to table. Error {
                    rows_resp.status_code
                    }: {
                        rows_resp.text
                        }"""
                )
    def update_rows(self, dataset_id, table_name, update_query):
        """
        Updates rows in an existing table in the specified dataset.

        Parameters:
            dataset_id (str): The ID of the dataset containing the table to update rows in.
            table_name (str): The name of the table to update rows in.
            update_query (str): A string representing the update query.

        Raises:
            ValueError: If the specified dataset or table does not exist in the workspace, or if the API returns an error.

        Returns:
            None
        """
        self.connect()

        # Check if the specified dataset and table exist in the workspace
        tables_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/tables'
        tables_resp = requests.get(tables_url, headers=self.headers)
        if tables_resp.status_code != 200:
            raise ValueError(f"Could not retrieve tables from Power BI API. Error {tables_resp.status_code}: {tables_resp.text}")
        tables = tables_resp.json()['value']
        table = next((t for t in tables if t['name'] == table_name), None)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist in dataset {dataset_id}.")

        # Prompt to verify the update
        print(f"Are you sure you want to update rows in table {table_name} in dataset {dataset_id}?")
        choice = input("Enter 'yes' to continue or 'no' to cancel: ")
        if choice.lower() != 'yes':
            print("Update cancelled.")
            return

        # Update the rows in the table
        update_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/tables/{table["id"]}/rows'
        update_resp = requests.patch(update_url, headers=self.headers, json={'updateDetails': update_query})
        if update_resp.status_code != 200:
            raise ValueError(f"Could not update rows in table. Error {update_resp.status_code}: {update_resp.text}")

    def delete_rows(self, dataset_id, table_name, delete_query):
        """
        Deletes rows from an existing table in the specified dataset.

        Parameters:
            dataset_id (str): The ID of the dataset containing the table to delete rows from.
            table_name (str): The name of the table to delete rows from.
            delete_query (str): A string representing the delete query.

        Raises:
            ValueError: If the specified dataset or table does not exist in the workspace, or if the API returns an error.

        Returns:
            None
        """
        self.connect()

        # Check if the specified dataset and table exist in the workspace
        tables_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/tables'
        tables_resp = requests.get(tables_url, headers=self.headers)
        if tables_resp.status_code != 200:
            raise ValueError(f"Could not retrieve tables from Power BI API. Error {tables_resp.status_code}: {tables_resp.text}")
        tables = tables_resp.json()['value']
        table = next((t for t in tables if t['name'] == table_name), None)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist in dataset {dataset_id}.")

        # Delete the rows from the table
        delete_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{dataset_id}/tables/{table["id"]}/rows'
        delete_resp = requests.delete(delete_url, headers=self.headers, json={'deleteDetails': delete_query})
        if delete_resp.status_code == 200:
            print(f"Rows deleted from table {table_name} in dataset {dataset_id}.")
        else:
            error = delete_resp.json()['error']
            message = error.get('message', 'Unknown error')
            raise ValueError(f"Could not delete rows from table. Error {delete_resp.status_code}: {message}")