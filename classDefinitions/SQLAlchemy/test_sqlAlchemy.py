import unittest
from unittest.mock import MagicMock, patch
from my_module import MSSQLDatabase

class TestMSSQLDatabase(unittest.TestCase):
    def setUp(self):
        self.db = MSSQLDatabase('connection_string', 'schema_name', 'table_name')

    @patch('my_module.create_engine')
    @patch('my_module.MetaData')
    def test_select_table(self, mock_metadata, mock_engine):
        conn = MagicMock()
        mock_engine.return_value.connect.return_value = conn

        table = MagicMock()
        table.c = {'col1': 'val1', 'col2': 'val2'}
        mock_metadata.return_value.reflect.return_value = None
        mock_metadata.return_value.tables = {'schema_name.table_name': table}

        result_set = [(1, 'foo'), (2, 'bar'), (3, 'baz')]
        conn.execute.return_value.fetchall.return_value = result_set

        select_cols = ['col1', 'col2']
        where_args = [{'type': 'where', 'column': 'col1', 'operator': '=', 'value': 'foo'}]
        group_by_args = [{'type': 'group_by', 'columns': ['col2']}]
        query = self.db.select_table(select_cols, *where_args, *group_by_args)
        expected_query = "SELECT col1, col2 FROM schema_name.table_name WHERE col1 = 'foo' GROUP BY col2"
        self.assertEqual(str(query), expected_query)
        self.assertEqual(query.compile().params, {'val_1': 'foo'})

        expected_result = [{'col1': 1, 'col2': 'foo'}, {'col1': 2, 'col2': 'bar'}, {'col1': 3, 'col2': 'baz'}]
        self.assertEqual(self.db.parse_results(result_set), expected_result)

    @patch('my_module.create_engine')
    @patch('my_module.MetaData')
    def test_update_table(self, mock_metadata, mock_engine):
        conn = MagicMock()
        mock_engine.return_value.connect.return_value = conn

        table = MagicMock()
        table.c = {'col1': 'val1', 'col2': 'val2'}
        mock_metadata.return_value.reflect.return_value = None
        mock_metadata.return_value.tables = {'schema_name.table_name': table}

        values = {'col1': 1, 'col2': 'foo'}
        where_args = [{'type': 'where', 'column': 'col1', 'operator': '=', 'value': 'foo'}]
        query = self.db.update_table(values, *where_args)
        expected_query = "UPDATE schema_name.table_name SET col1 = 1, col2 = 'foo' WHERE col1 = 'foo'"
        self.assertEqual(str(query), expected_query)
        self.assertEqual(query.compile().params, {'val_1': 'foo', 'val_2': 1, 'val_3': 'foo'})

    @patch('my_module.create_engine')
    @patch('my_module.MetaData')
    def test_append_table(self, mock_metadata, mock_engine):
        conn = MagicMock()
        mock_engine.return_value.connect.return_value = conn

        table = MagicMock()
        table.c = {'col1': 'val1', 'col2': 'val2'}
        mock_metadata.return_value.reflect.return_value = None
        mock_metadata.return_value.tables = {'schema_name.table_name': table}

        data = [{'col1': 1, 'col2': 'foo'}, {'col1': 2, 'col2': 'bar'}]