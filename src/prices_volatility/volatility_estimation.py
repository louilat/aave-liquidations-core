import pandas as pd
from pandas import DataFrame
import numpy as np
from sklearn.linear_model import LinearRegression


def preprocess_prices_for_fitting(prices: DataFrame):
    prices_ = prices[
        prices.UnderlyingToken == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    ].sort_values("Timestamp")[["Timestamp"]]
    tokens_list = prices.UnderlyingToken.unique().tolist()
    for token in tokens_list:
        print(f"      - Treating token {token}...")
        # Remove multiplicative constant + take log
        price_token = prices[prices.UnderlyingToken == token].copy()
        price_token["Time"] = pd.to_datetime(price_token.Timestamp, unit="s")
        price_token.Price = price_token.Price / 1e8
        price_token = price_token.sort_values("Time").reset_index(drop=True)
        price_token["bm"] = price_token.Price / price_token.Price[0]
        price_token["bm"] = np.log(price_token["bm"])

        # Remove trend
        reg = LinearRegression(fit_intercept=False).fit(
            np.array([price_token.index.values]).transpose(), price_token["bm"]
        )
        slope = reg.coef_[0]
        price_token["bm"] = price_token["bm"] - price_token.index * slope

        # Compute diff
        price_token[token] = price_token.bm.diff()

        prices_ = prices_.merge(
            price_token[["Timestamp", token]], how="left", on="Timestamp"
        )

    prices_ = prices_.sort_values("Timestamp").reset_index(drop=True)
    prices_ = prices_[1:]  # Drop first NA induced by diff()

    prices_ = prices_.set_index(prices_.Timestamp).drop(columns="Timestamp")

    # Remove token columns with at least one nana
    for col in prices_.columns:
        if prices_[col].isnull().sum() > 0:
            prices_ = prices_.drop(columns=col)

    return prices_


def fit_multivariate_normal_distribution(brownian_motions: np.ndarray):
    w = np.array([k for k in range(len(brownian_motions) - 1, -1, -1)])
    w = np.exp(-w / 62)
    Cov = np.cov(brownian_motions, rowvar=False, aweights=w)
    std = np.sqrt(np.diag(Cov) * 365)
    Sigma = Cov * 365 / (np.array([std]).transpose() * np.array([std]))
    np.fill_diagonal(Sigma, std)
    return Sigma


def generate_prices_correlations(corr_matrix: np.ndarray, reserves_list: list):
    prices_correlations = DataFrame()
    for i, row in enumerate(corr_matrix):
        corr = DataFrame(
            {
                "pair1": reserves_list[i],
                "pair2": reserves_list,
                "rho": row,
            }
        )
        prices_correlations = pd.concat((prices_correlations, corr))
    return prices_correlations.set_index(["pair1", "pair2"])
