from datetime import datetime
import pandas as pd
from pandas import DataFrame
import requests


def get_reserves_data(day: datetime) -> DataFrame:
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/reserves",
        params={"date": day_str},
        verify=False,
    )
    reserves = pd.json_normalize(resp.json())
    return reserves


def get_reserves_data_updated(day: datetime) -> DataFrame:
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/events/reservedataupdated",
        params={"date": day_str},
        verify=False,
    )
    reserves_data_updated = pd.json_normalize(resp.json())
    return reserves_data_updated
