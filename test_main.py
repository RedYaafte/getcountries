import pytest

import pandas as pd
from main import millisecond_time, get_countries, get_dataframe


@pytest.fixture
def countries():
    return [
        {
            'Capital': 'Kuala Lumpur',
            'Region': 'Asia',
            'Language': {'eng': 'English', 'msa': 'Malay'}
        }
    ]


def test_second_to_millisecond():
    assert millisecond_time(seconds=2) == 2000.00


def test_get_countries(countries):
    assert get_countries()[0] == countries[0]


def test_get_data_frame(countries):
    df1 = pd.DataFrame(countries[0])
    df2 = get_dataframe(countries)
    assert df1['Capital'][0] == df2['Capital'][0]
