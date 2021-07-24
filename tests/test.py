import pytest
import urllib.error
from etl import extract_transform_covid_data

test_url1 = 'http://www.google.com/nonexistantfile'
test_url2 = 'http://www.amazon.com/yousillygoose'


def test_http_error():
    with pytest.raises(urllib.error.HTTPError):
        extract_transform_covid_data(test_url1, test_url2)
