import boto3
import pytest
import os
from moto import mock_dynamodb2
from data_handler import get_most_recent_date, add_rows_to_database
import pandas as pd

dataframe_dict = {
    'date': ['2021-8-1', '2021-8-2', '2021-8-3'],
    'cases': [123, 234, 345],
    'deaths': [12, 23, 34],
    'recovered': [56, 67, 78]
}
dataframe = pd.DataFrame(data=dataframe_dict)
dataframe['date'] = pd.to_datetime(dataframe['date'])


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def ddb(aws_credentials):
    with mock_dynamodb2():
        ddb = boto3.resource('dynamodb', region_name='us-east-1')
        yield ddb


def set_up_test_table(ddb):
    ddb.create_table(
        TableName='test-table',
        KeySchema=[
            {
                'AttributeName': 'date',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'date',
                'AttributeType': 'S'
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    table = ddb.Table('test-table')
    return table


@mock_dynamodb2
def test_get_most_recent_date(ddb):
    rows_to_add = dataframe
    table = set_up_test_table(ddb)
    for index, row in rows_to_add.iterrows():
        table.put_item(
            Item={
                'date': str(row.date),
                'cases': row.cases,
                'deaths': row.deaths,
                'recovered': row.recovered,
            }
        )

    most_recent_date = get_most_recent_date(table)

    assert str(most_recent_date) == '2021-08-03 00:00:00'


def test_add_rows_to_database(ddb):
    rows_to_add = dataframe
    table = set_up_test_table(ddb)

    rows_added = add_rows_to_database(rows_to_add, table)
    response = table.get_item(Key={'date': '2021-08-02 00:00:00'})

    assert rows_added == 3
    assert response['Item']['recovered'] == 67
