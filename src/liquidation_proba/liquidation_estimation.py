from pandas import DataFrame
import numpy as np
from scipy.stats import norm


def compute_liquidation_proba(
    user_balances: DataFrame, prices_volatility: DataFrame, detla_t: float
) -> float:
    all_combinaisons = user_balances[["underlyingAsset", "name", "a"]].merge(
        user_balances[["underlyingAsset", "name", "a"]],
        how="cross",
        suffixes=["From", "To"],
    )
    std = prices_volatility.reset_index()
    std = std[std.pair1 == std.pair2]
    std = std[["pair1", "rho"]].rename(columns={"pair1": "underlyingAsset"})
    all_combinaisons = (
        all_combinaisons.merge(
            std,
            how="left",
            left_on="underlyingAssetFrom",
            right_on="underlyingAsset",
        )
        .drop(columns="underlyingAsset")
        .rename(columns={"rho": "stdFrom"})
    )
    all_combinaisons = (
        all_combinaisons.merge(
            std,
            how="left",
            left_on="underlyingAssetTo",
            right_on="underlyingAsset",
        )
        .drop(columns="underlyingAsset")
        .rename(columns={"rho": "stdTo"})
    )

    corr = prices_volatility.reset_index()
    corr["rho"] = np.where(
        corr.pair1 == corr.pair2,
        1,
        corr.rho,
    )
    corr = corr.rename(
        columns={
            "pair1": "underlyingAssetFrom",
            "pair2": "underlyingAssetTo",
        }
    )
    all_combinaisons = all_combinaisons.merge(
        corr, how="left", on=["underlyingAssetFrom", "underlyingAssetTo"]
    )
    all_combinaisons["value"] = (
        all_combinaisons.aFrom
        * all_combinaisons.aTo
        * all_combinaisons.stdFrom
        * all_combinaisons.stdTo
        * all_combinaisons.rho
        * detla_t
    )
    q_value = user_balances.a.sum() / np.sqrt(all_combinaisons.value.sum())
    return norm.cdf(q_value)


def compute_liquidation_proba_trajectory(
    user_balances: DataFrame, volatility: DataFrame, detla_t: float
) -> DataFrame:
    probas = user_balances.groupby(["BlockNumber", "Timestamp"]).apply(
        lambda x: compute_liquidation_proba(x, volatility, detla_t)
    )
    probas = probas.reset_index().rename(columns={0: "proba_p1"})
    probas["proba_p2"] = np.minimum(1, 2 * probas.proba_p1)
    return probas


def compute_health_factor_trajectory(user_balances: DataFrame) -> DataFrame:
    balances_ = user_balances.copy()
    balances_["hf_numerator"] = (
        balances_.currentATokenBalanceUSD
        * balances_.reserveLiquidationThreshold
        * balances_.collateral_enabled
    )
    balances_ = balances_.groupby(["BlockNumber", "Timestamp"], as_index=False).agg(
        {"hf_numerator": "sum", "currentVariableDebtUSD": "sum"}
    )
    balances_["hf"] = np.where(
        balances_.currentVariableDebtUSD == 0,
        np.inf,
        balances_.hf_numerator
        / np.where(
            balances_.currentVariableDebtUSD == 0,
            np.nan,
            balances_.currentVariableDebtUSD,
        ),
    )
    return balances_[["BlockNumber", "Timestamp", "hf"]]
