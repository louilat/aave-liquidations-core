import pandas as pd
from datetime import datetime
import requests


def get_liquidated_users_list(day: datetime) -> list[tuple[str, str]]:
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/events/liquidation",
        params={"date": day_str},
        verify=False,
    )
    liquidations = pd.json_normalize(resp.json())
    if liquidations.empty:
        return []
    return list(zip(liquidations.blockNumber, liquidations.user))
