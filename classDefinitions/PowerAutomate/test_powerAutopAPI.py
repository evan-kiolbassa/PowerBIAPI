import unittest
import requests
from unittest.mock import patch
from classDefinitions.PowerAutomate.powerAutoAPI import PowerAutomateScheduler


class TestPowerAutomateScheduler(unittest.TestCase):
    def setUp(self):
        self.client_id = 'test_client_id'
        self.client_secret = 'test_client_secret'
        self.tenant_id = 'test_tenant_id'
        self.subscription_id = 'test_subscription_id'
        self.resource_group = 'test_resource_group'
        self.factory_name = 'test_factory_name'

        self.power_automate_scheduler = PowerAutomateScheduler(
            self.client_id,
            self.client_secret,
            self.tenant_id,
            self.subscription_id,
            self.resource_group,
            self.factory_name
        )

    @patch('requests.post')
    def test_connect(self, mock_post):
        mock_response = requests.models.Response()
        mock_response.status_code = 200
        mock_response.json = lambda: {'access_token': 'test_access_token'}

        mock_post.return_value = mock_response

        self.power_automate_scheduler.connect()

        self.assertIsNotNone(self.power_automate_scheduler.token)

    def test_build_schedule_dict_refresh(self):
        expected_result = {
            'properties': {
                'recurrence': {
                    'interval': 1,
                    'frequency': 'Day',
                    'startTime': '2022-02-22T01:00:00Z',
                    'timeZone': 'UTC',
                    'schedule': 'UTC',
                },
                'dataSource': {
                    'type': 'PowerBI',
                    'dataSourceType': 'Table',
                    'dataSourceObjectId': 'test_table_name',
                    'dataSourceObjectPath': 'test_dataset_id/test_table_name',
                },
                'pipeline': {
                    'name': 'test_schedule_name',
                    'description': 'Scheduled task created by PowerAutomateScheduler',
                    'activities': [
                        {
                            'name': 'RefreshData',
                            'type': 'WebActivity',
                            'linkedServiceName': 'PowerBI',
                            'typeProperties': {
                                'url': 'https://api.powerbi.com/v1.0/myorg/groups/test_dataset_id/datasets/test_table_name/refreshes',
                                'method': 'POST'
                            },
                        },
                    ]
                },
            },
            'type': 'Microsoft.DataFactory/factories/pipelines',
        }

        result = self.power_automate_scheduler._build_schedule_dict(
            dataset_id='test_dataset_id',
            table_name='test_table_name',
            schedule_name='test_schedule_name',
            recurrence_pattern='UTC',
            recurrence_interval=1,
            recurrence_frequency='Day',
            timezone='UTC',
            startTime='2022-02-22T01:00:00Z',
            description='Scheduled task created by PowerAutomateScheduler',
            isRefresh=True,
        )

        self.assertEqual(expected_result, result)

    def test_build_schedule_dict_script(self):
        expected_result = {
            'properties': {
                'recurrence': {
                    'interval': 1,
                    'frequency': 'Day',
                    'startTime': '2022-02-22T01:00:00Z',
                    'timeZone': 'UTC',
                    'schedule': 'UTC',
                },
                'pipeline': {
                    'name': 'test_schedule_name',
                    'description': 'Scheduled task created by PowerAutomateScheduler',
                    'activities': [
                        {
                            'name': 'RunPythonScript',
                            'type': 'HDInsightHive',
                            'linkedServiceName': 'HDInsight',
                            'typeProperties': {
                                'scriptPath': '/path/to/script.py',
                            },
                        }
                        ],
            },
        },
        'type': 'Microsoft.DataFactory/factories/pipelines',
    }

        result = self.power_automate_scheduler._build_schedule_dict(
            schedule_name='test_schedule_name',
            recurrence_pattern='UTC',
            recurrence_interval=1,
            recurrence_frequency='Day',
            timezone='UTC',
            startTime='2022-02-22T01:00:00Z',
            description='Scheduled task created by PowerAutomateScheduler',
            script='/path/to/script.py',
        )

        self.assertEqual(expected_result, result)

    @patch('requests.put')
    def test_schedule_refresh(self, mock_put):
        mock_response = requests.models.Response()
        mock_response.status_code = 201

        mock_put.return_value = mock_response

        self.power_automate_scheduler.token = 'test_access_token'

        self.power_automate_scheduler.schedule(
            dataset_id='test_dataset_id',
            table_name='test_table_name',
            schedule_name='test_schedule_name',
            recurrence_pattern='UTC',
            recurrence_interval=1,
            recurrence_frequency='Day',
            timezone='UTC',
            startTime='2022-02-22T01:00:00Z',
        )

        self.assertEqual(mock_put.call_count, 1)

    @patch('requests.put')
    def test_schedule_script(self, mock_put):
        mock_response = requests.models.Response()
        mock_response.status_code = 201

        mock_put.return_value = mock_response

        self.power_automate_scheduler.token = 'test_access_token'

        self.power_automate_scheduler.schedule_script(
            schedule_name='test_schedule_name',
            recurrence_pattern='UTC',
            recurrence_interval=1,
            recurrence_frequency='Day',
            timezone='UTC',
            startTime='2022-02-22T01:00:00Z',
            script='/path/to/script.py',
        )

        self.assertEqual(mock_put.call_count, 1)
