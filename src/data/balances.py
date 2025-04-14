import pandas as pd
from pandas import DataFrame
from datetime import datetime
import requests


def get_user_balances(user: str, day: datetime) -> DataFrame:
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/user-selec-balances",
        params={"date": day_str, "user": user},
        verify=False,
    )
    return pd.json_normalize(resp.json())


def compute_user_balances(
    balances_before: DataFrame,
    balances_after: DataFrame,
    prices: DataFrame,
    liquidation_block: int,
) -> DataFrame:
    prices_ = prices.copy()
    prices_.BlockNumber = prices_.BlockNumber.apply(int)
    prices_before = prices_[prices_.BlockNumber < liquidation_block]
    prices_after = prices_[prices_.BlockNumber >= liquidation_block]
    balances_before_ = prices_before.merge(
        balances_before,
        how="left",
        left_on="UnderlyingToken",
        right_on="underlyingAsset",
    ).dropna(subset="user_address")
    balances_after_ = prices_after.merge(
        balances_after,
        how="left",
        left_on="UnderlyingToken",
        right_on="underlyingAsset",
    ).dropna(subset="user_address")
    balances = pd.concat((balances_before_, balances_after_))
    return balances


def process_user_balances(user_balances: DataFrame, reserves: DataFrame) -> DataFrame:
    balances = user_balances.merge(
        reserves[
            [
                "underlyingAsset",
                "reserveLiquidationThreshold",
                "liquidityIndex",
                "variableBorrowIndex",
            ]
        ],
        how="left",
        on="underlyingAsset",
    )

    balances["currentATokenBalanceUSD"] = (
        balances.scaledATokenBalance.apply(int)
        * balances.liquidityIndex.apply(int)
        * 1e-27
        * balances.Price
        * 1e-8
        / 10**balances.decimals
    )
    balances["currentVariableDebtUSD"] = (
        balances.scaledVariableDebt.apply(int)
        * balances.variableBorrowIndex.apply(int)
        * 1e-27
        * balances.Price
        * 1e-8
        / 10**balances.decimals
    )

    balances.reserveLiquidationThreshold = balances.reserveLiquidationThreshold * 1e-4

    balances["a"] = (
        balances.currentVariableDebtUSD
        - balances.reserveLiquidationThreshold * balances.currentATokenBalanceUSD
    )

    select_columns = [
        "BlockNumber",
        "Timestamp",
        "underlyingAsset",
        "name",
        "currentATokenBalanceUSD",
        "currentVariableDebtUSD",
        "reserveLiquidationThreshold",
        "a",
    ]

    return balances[select_columns]
