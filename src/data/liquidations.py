import pandas as pd
from pandas import DataFrame
from datetime import datetime
import requests


def get_liquidations_params(client_s3):
    liquidations_params = pd.read_csv(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key="liquidations/liquidations_params.csv",
        )["Body"]
    )
    return liquidations_params


def get_liquidations(day: datetime) -> DataFrame:
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/events/liquidation",
        params={"date": day_str},
        verify=False,
    )
    liquidation = pd.json_normalize(resp.json())
    return liquidation


# def get_liquidated_users_list(day: datetime) -> list[tuple[str, str]]:
#     month = day.ctime()[4:7]
#     day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
#     resp = requests.get(
#         "https://aavedata.lab.groupe-genes.fr/events/liquidation",
#         params={"date": day_str},
#         verify=False,
#     )
#     liquidations = pd.json_normalize(resp.json())
#     if liquidations.empty:
#         return []
#     return list(zip(liquidations.blockNumber, liquidations.user))
