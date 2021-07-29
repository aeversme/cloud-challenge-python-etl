import pytest
import urllib.error
from etl import download_covid_dataframe


test_url1 = 'http://www.google.com/nonexistantfile'
test_url2 = 'http://www.amazon.com/yousillygoose'


def test_http_error():
    with pytest.raises(urllib.error.HTTPError):
        download_covid_dataframe(test_url1, test_url2)
