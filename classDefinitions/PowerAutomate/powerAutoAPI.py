import requests

class PowerAutomateScheduler:
    def __init__(self, client_id, client_secret, tenant_id, subscription_id, resource_group, factory_name, api_version='2016-06-01'):
        """
        Initializes a new instance of the PowerAutomateScheduler class.

        Parameters:
            client_id (str): The client ID for your Azure AD application.
            client_secret (str): The client secret for your Azure AD application.
            tenant_id (str): The ID of your Azure AD tenant.
            api_version (str, optional): The version of the Power Automate API to use. Defaults to '2016-06-01'.

        Returns:
            None
        """

        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.api_version = api_version
        self.token = None
        self.headers = {'Content-Type': 'application/json'}

    def connect(self):
        """
        Authenticates with the Power Automate API using the client ID and client secret.

        Raises:
            ValueError: If authentication fails.

        Returns:
            None
        """
        # Get an access token for the API
        auth_url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token'
        auth_resp = requests.post(auth_url, data={
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': 'https://management.azure.com/',
        })
        if auth_resp.status_code != 200:
            raise ValueError(f"Could not authenticate with Power Automate API. Error {auth_resp.status_code}: {auth_resp.text}")
        self.token = auth_resp.json()['access_token']
        self.headers['Authorization'] = f'Bearer {self.token}'

    def _build_schedule_dict(
        self,
        dataset_id=None,
        table_name=None,
        schedule_name=None,
        recurrence_pattern=None,
        recurrence_interval=None,
        recurrence_frequency=None,
        timezone=None,
        startTime=None,
        description='Scheduled task created by PowerAutomateScheduler',
        isRefresh=False,
        script=None
        ):

            schedule = {
                'properties': {
                    'recurrence': {
                        'interval': recurrence_interval,
                        'frequency': recurrence_frequency,
                        'startTime': startTime,
                        'timeZone': timezone,
                        'schedule': recurrence_pattern,
                    },
                    'dataSource': {
                        'type': 'PowerBI',
                        'dataSourceType': 'Table',
                        'dataSourceObjectId': table_name,
                        'dataSourceObjectPath': f'{dataset_id}/{table_name}',
                    },
                    'pipeline': {
                        'name': schedule_name,
                        'description': description,

                    },
                },
                'type': 'Microsoft.DataFactory/factories/pipelines',
            }
            try:
                if isRefresh is True and script is None:
                    schedule['pipeline']['activities'] = [
                                {
                                    'name': 'RefreshData',
                                    'type': 'WebActivity',
                                    'linkedServiceName': 'PowerBI',
                                    'typeProperties': {
                                        'url': f'''https://api.powerbi.com/v1.0/myorg/groups/{
                                            dataset_id
                                            }/datasets/{
                                                table_name
                                                }/refreshes''',
                                        'method': 'POST'
                                    },
                                },
                            ]
            except:
                raise ValueError
                print('If isRefresh is True, there must not be a script argument')
            try:
                if script is not None and isRefresh is False:
                    schedule['pipeline']['activities'] = [
                            {
                                'name': 'RunPythonScript',
                                'type': 'HDInsightHive',
                                'linkedServiceName': 'HDInsight',
                                'typeProperties': {
                                'scriptPath': script,
                                }
                            },
                        ]
            except:
                raise ValueError
                print('If an argument for script is passed, ')



            return schedule

    def schedule(
        self,
        dataset_id=None,
        table_name=None,
        schedule_name=None,
        recurrence_pattern=None,
        recurrence_interval=None,
        recurrence_frequency=None,
        timezone=None,
        startTime=None,
        description='Scheduled task created by PowerAutomateScheduler'
        ):
        """
        Schedules a task to refresh a Power BI table at a regular interval.

        Parameters:
            dataset_id (str): The ID of the dataset containing the table to refresh.
            table_name (str): The name of the table to refresh.
            schedule_name (str): The name of the schedule to create.
            recurrence_pattern (str): A string representing the recurrence pattern for the schedule.
            timezone (str): The timezone to use for the schedule.

        Raises:
            ValueError: If the schedule request fails.

        Returns:
            None
        """
        self.connect()

        # Define the schedule request body
        schedule = self._build_schedule(
            dataset_id,
            table_name,
            schedule_name,
            recurrence_pattern,
            timezone,
            recurrence_interval,
            recurrence_frequency,
            startTime,
            description
            )

        # Submit the schedule request
        create_pipeline_url = f'''https://management.azure.com/subscriptions/{
            self.subscription_id
            }/resourceGroups/{
                self.resource_group
                }/providers/Microsoft.DataFactory/factories/{
                    self.factory_name
                    }/pipelines/{
                        schedule_name
                        }?api-version={
                            self.api_version
                            }'''
        create_pipeline_resp = requests.put(create_pipeline_url, headers=self.headers, json=schedule)
        if create_pipeline_resp.status_code != 201:
            raise ValueError(f"Could not create schedule in Power Automate. Error {create_pipeline_resp.status_code}: {create_pipeline_resp.text}")

    def schedule_script(
        self,
        schedule_name=None,
        recurrence_pattern=None,
        recurrence_interval=None,
        recurrence_frequency=None,
        timezone=None,
        startTime=None,
        description='Scheduled task created by PowerAutomateScheduler',
        script='/path/to/script.py'
        ):
        """
        Schedule a Python script to run on a schedule using the Power Automate API.

        Parameters:
            script_path (str): The local file path to the Python script to be scheduled.
            schedule_name (str): The name to assign to the scheduled task.
            recurrence_pattern (str): A CRON expression defining the schedule on which the task should run.
            timezone (str): The timezone in which the task should run.

        Raises:
            ValueError: If the specified script file does not exist, or if the API returns an error.

        Returns:
            None
        """
        self.connect()

        schedule = self._build_schedule(
            schedule_name,
            recurrence_pattern,
            timezone,
            recurrence_interval,
            recurrence_frequency,
            startTime,
            description,
            script
            )

        # Submit the schedule request
        create_pipeline_url = f'''https://management.azure.com/subscriptions/{
            self.subscription_id
            }/resourceGroups/myResourceGroup/providers/Microsoft.DataFactory/factories/{
                self.factory_name
                }/pipelines/{
                    schedule_name
                    }?api-version={self.api_version}'''
        create_pipeline_resp = requests.put(create_pipeline_url, headers=self.headers, json=schedule)
        if create_pipeline_resp.status_code != 201:
            raise ValueError(f"Could not create schedule in Power Automate. Error {create_pipeline_resp.status_code}: {create_pipeline_resp.text}")
