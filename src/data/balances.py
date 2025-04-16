import pandas as pd
from pandas import DataFrame
from datetime import datetime
import requests
import numpy as np
from web3 import contract


def get_user_balances(user: str, day: datetime) -> DataFrame:
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    resp = requests.get(
        "https://aavedata.lab.groupe-genes.fr/user-selec-balances",
        params={"date": day_str, "user": user},
        verify=False,
    )
    return pd.json_normalize(resp.json())


def get_user_events(user: str, day: datetime):
    all_user_events = []
    month = day.ctime()[4:7]
    day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
    for event in ["supply", "borrow", "withdraw", "repay"]:
        resp = requests.get(
            f"https://aavedata.lab.groupe-genes.fr/events/{event}",
            params={"date": day_str},
            verify=False,
        )
        if event in ["withdraw", "repay"]:
            user_events = [ev for ev in resp.json() if ev["user"] == user]
        else:
            user_events = [ev for ev in resp.json() if ev["onBehalfOf"] == user]
        for ev in user_events:
            ev["action"] = event
        all_user_events.extend(user_events)

    # AToken transfer
    resp = requests.get(
        f"https://aavedata.lab.groupe-genes.fr/events/balancetransfer",
        params={"date": day_str},
        verify=False,
    )
    # Send
    user_events = [ev for ev in resp.json() if ev["from"] == user]
    for ev in user_events:
        ev["action"] = "balancetransfer_send"
    all_user_events.extend(user_events)
    # Receive
    user_events = [ev for ev in resp.json() if ev["to"] == user]
    for ev in user_events:
        ev["action"] = "balancetransfer_receive"
    all_user_events.extend(user_events)

    try:
        all_user_events = pd.json_normalize(all_user_events)[
            ["blockNumber", "reserve", "action", "amount"]
        ]
    except KeyError:
        return pd.DataFrame(
            {"blockNumber": [], "reserve": [], "action": [], "amount": []}
        )
    return all_user_events.sort_values("blockNumber")


def add_liquidation_to_user_events(user_events, liquidation_events, liquidation_params):
    liquidation_events_ = liquidation_events.merge(
        liquidation_params, left_on="collateralAsset", right_on="reserve"
    )
    for _, liq in liquidation_events_.iterrows():
        block = liq["blockNumber"]
        colAmount = liq["liquidatedCollateralAmount"]
        withdraw_amount = (
            colAmount
            + (colAmount - colAmount / (liq["liquidationBonus"] * 1e-4))
            * liq["liquidationProtocolFee"]
            * 1e-4
        )
        withdraw_reserve = liq["collateralAsset"]
        repay_amount = liq["debtToCover"]
        repay_reserve = liq["debtAsset"]

        user_events.loc[len(user_events)] = {
            "blockNumber": block,
            "reserve": withdraw_reserve,
            "action": "withdraw",
            "amount": withdraw_amount,
        }
        user_events.loc[len(user_events)] = {
            "blockNumber": block,
            "reserve": repay_reserve,
            "action": "repay",
            "amount": repay_amount,
        }


def _find_closest_indexes(row, reserves_data_updated, reserves, liquidityIndex):
    asset = row["UnderlyingToken"]
    block = row["BlockNumber"]
    indexes = reserves_data_updated[reserves_data_updated.reserve == asset].reset_index(
        drop=True
    )

    try:
        idx = np.argmin(np.abs(indexes.blockNumber - block))
    except ValueError:
        if liquidityIndex:
            return (
                int(
                    reserves.loc[
                        reserves.underlyingAsset == asset, "liquidityIndex"
                    ].item()
                )
                * 1e-27
            )
        return (
            int(
                reserves.loc[
                    reserves.underlyingAsset == asset, "variableBorrowIndex"
                ].item()
            )
            * 1e-27
        )

    if liquidityIndex:
        return int(indexes.loc[idx, "liquidityIndex"]) * 1e-27
    return int(indexes.loc[idx, "variableBorrowIndex"]) * 1e-27


def compute_user_balances(
    user_initial_balance, day_prices, user_events, reserves_data_updated, reserves
):
    prices = day_prices.copy()
    prices.BlockNumber = prices.BlockNumber.apply(int)

    balances = prices.merge(
        user_initial_balance,
        how="left",
        left_on="UnderlyingToken",
        right_on="underlyingAsset",
    ).dropna(subset="user_address")

    balances["liquidityIndex"] = balances.apply(
        _find_closest_indexes, axis=1, args=(reserves_data_updated, reserves, True)
    )
    balances["variableBorrowIndex"] = balances.apply(
        _find_closest_indexes, axis=1, args=(reserves_data_updated, reserves, False)
    )

    balances["currentATokenBalance"] = (
        balances.scaledATokenBalance * balances.liquidityIndex
    )
    balances["currentVariableDebt"] = (
        balances.scaledVariableDebt * balances.variableBorrowIndex
    )
    for _, event in user_events.iterrows():
        block = event["blockNumber"]
        amount = event["amount"]
        asset = event["reserve"]
        future_reserve_mask = (balances.BlockNumber >= block) & (
            balances.underlyingAsset == asset
        )
        try:
            liquidityIndex = balances[future_reserve_mask].reset_index().liquidityIndex[0]
        except KeyError as e:
            print(e)
            liquidityIndex = np.nan
    
        if event["action"] == "supply":
            balances.loc[future_reserve_mask, "currentATokenBalance"] += amount
        elif event["action"] == "borrow":
            balances.loc[future_reserve_mask, "currentVariableDebt"] += amount
        elif event["action"] == "withdraw":
            balances.loc[future_reserve_mask, "currentATokenBalance"] -= amount
        elif event["action"] == "repay":
            balances.loc[future_reserve_mask, "currentVariableDebt"] -= amount
        elif event["action"] == "balancetransfer_send":
            balances.loc[future_reserve_mask, "currentATokenBalance"] -= (
                amount * liquidityIndex
            )
        elif event["action"] == "balancetransfer_receive":
            balances.loc[future_reserve_mask, "currentATokenBalance"] += (
                amount * liquidityIndex
            )

    balances["currentATokenBalanceUSD"] = (
        balances.currentATokenBalance / 10**balances.decimals * balances.Price * 1e-8
    )
    balances["currentVariableDebtUSD"] = (
        balances.currentVariableDebt / 10**balances.decimals * balances.Price * 1e-8
    )

    return balances


def _is_user_collateral_enabled(
    pool: contract,
    user: str,
    asset: str,
    block_number: int,
    liquidation_params: DataFrame,
) -> bool:
    asset_id = liquidation_params.loc[liquidation_params.reserve == asset, "id"].item()
    user_cfig = pool.functions.getUserConfiguration(user).call(
        block_identifier=block_number
    )[0]
    bin_data = bin(user_cfig)[2:]
    if len(bin_data) % 2 != 0:
        bin_data = "0" + bin_data
    if len(bin_data) < 2 * (asset_id + 1):
        return False
    if asset_id == 0:
        data = bin_data[-2 * (asset_id + 1) :]
    else:
        data = bin_data[-2 * (asset_id + 1) : -2 * asset_id]
    return bool(int(data[0]))


def process_user_balances(
    user: str,
    user_balances: DataFrame,
    reserves: DataFrame,
    pool: contract,
    liquidation_params: DataFrame,
) -> DataFrame:
    """
    Clean the user_balances data by doing the following:
        1. For each asset used by the user, indicates if the asset is
            enabled as collateral by the user in the column "collateral_enabled"
        2. Add the "reserveLiquidationThreshold info by merging the user_balancances
            with the reserves_data dataframe
        3. Computes the "a" value sum_r(b - a*LT)

    Args:
        user (str): The user address
        user_balances (DataFrame): Output from `compute_user_balances()` function
        reserves (DataFrame): The reserves_data dataframe
        pool (web3.contract): The Aave Pool contract
        liquidation_params: DataFrame containing the reserves ids.

    Returns:
        (DataFrame): The cleaned and completed user_balances.

    """

    # Is collateral enabled
    refBlock = int(user_balances.BlockNumber.apply(int).min())
    collateral_enabled = []
    user_assets = user_balances.underlyingAsset.unique().tolist()
    for asset in user_assets:
        enabled = _is_user_collateral_enabled(
            pool=pool,
            user=user,
            asset=asset,
            block_number=refBlock,
            liquidation_params=liquidation_params,
        )
        collateral_enabled.append(enabled)

    print("Assets enabled as collateral by user: ", collateral_enabled)
    collateral_policy = pd.DataFrame(
        {"underlyingAsset": user_assets, "collateral_enabled": collateral_enabled}
    )
    balances = user_balances.merge(collateral_policy, how="left", on="underlyingAsset")

    # Add reserveLiquidationThreshold
    balances = balances.merge(
        reserves[
            [
                "underlyingAsset",
                "reserveLiquidationThreshold",
            ]
        ],
        how="left",
        on="underlyingAsset",
    )

    balances.reserveLiquidationThreshold = balances.reserveLiquidationThreshold * 1e-4

    # Compute the a value
    balances["a"] = (
        balances.currentVariableDebtUSD
        - balances.reserveLiquidationThreshold
        * balances.currentATokenBalanceUSD
        * balances.collateral_enabled
    )

    select_columns = [
        "BlockNumber",
        "Timestamp",
        "underlyingAsset",
        "name",
        "collateral_enabled",
        "currentATokenBalanceUSD",
        "currentVariableDebtUSD",
        "reserveLiquidationThreshold",
        "a",
    ]

    return balances[select_columns]
