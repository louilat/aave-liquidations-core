import pandas as pd
from pandas import DataFrame
import numpy as np
from datetime import datetime, timedelta
import requests


def get_daily_prices(start: datetime, stop: datetime) -> DataFrame:
    all_prices = DataFrame()
    day = start
    while day <= stop:
        month = day.ctime()[4:7]
        day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
        resp = requests.get(
            "https://aavedata.lab.groupe-genes.fr/prices",
            params={"date": day_str},
            verify=False,
        )
        day_prices = pd.json_normalize(resp.json())
        day_prices = day_prices[day_prices.Timestamp == np.min(day_prices.Timestamp)]
        all_prices = pd.concat((all_prices, day_prices))
        day += timedelta(days=1)
    return all_prices


def get_hourly_prices(day: datetime):
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/prices",
        params={"date": day_str},
        verify=False,
    )
    day_prices = pd.json_normalize(resp.json())
    return day_prices
