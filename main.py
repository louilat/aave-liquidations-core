from datetime import datetime, timedelta
import warnings
import pandas as pd
from pandas import DataFrame
import boto3
import io
import os
import json
from web3 import Web3

warnings.filterwarnings("ignore")

from src.data.liquidations import get_liquidations_params, get_liquidations
from src.data.prices import get_daily_prices, get_hourly_prices
from src.data.balances import (
    get_user_balances,
    get_user_events,
    add_liquidation_to_user_events,
    compute_user_balances,
    process_user_balances,
)
from src.data.reserves import get_reserves_data, get_reserves_data_updated
from src.prices_volatility.volatility_estimation import (
    preprocess_prices_for_fitting,
    fit_multivariate_normal_distribution,
    generate_prices_correlations,
)
from src.liquidation_proba.liquidation_estimation import (
    compute_liquidation_proba_trajectory,
    compute_health_factor_trajectory,
)


# Run Parameters
output_path = "try/liquidation_trajectories/"
start = datetime(2024, 4, 3)
stop = datetime(2024, 4, 3)
vol_estimation_nb_days = 62
delta_t = 1 / 365


print("Starting Job...")

w3 = Web3(Web3.HTTPProvider(os.environ["NODE_PROVIDER"]))

with open("src/abi/pool.abi") as file:
    pool_abi = json.load(file)

pool = w3.eth.contract(
    address="0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2", abi=pool_abi
)

client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio-simple.lab.groupe-genes.fr",
    aws_access_key_id=os.environ["ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"],
    verify=False,
)


liquidations_params = get_liquidations_params(client_s3=client_s3)

day = start
while day <= stop:
    print("***Treating day: ", day, "***")

    # Reserves and raw prices data
    reserves = get_reserves_data(day=day)
    liquidation_day_prices = get_hourly_prices(day=day)
    volatility_estimation_prices = get_daily_prices(
        start=day - timedelta(days=vol_estimation_nb_days),
        stop=day,
    )

    # Compute prices volatility
    processed_prices = preprocess_prices_for_fitting(
        prices=volatility_estimation_prices
    )
    Sigma = fit_multivariate_normal_distribution(
        brownian_motions=processed_prices.values
    )
    volatility = generate_prices_correlations(
        corr_matrix=Sigma, reserves_list=processed_prices.columns.tolist()
    )

    reserves_data_updated = get_reserves_data_updated(day=day)

    day_trajectories = DataFrame()
    day_user_balances = DataFrame()
    liquidations_day = get_liquidations(day=day)
    for liquidated_user in liquidations_day.user.unique().tolist():
        print("User is: ", liquidated_user)
        liquidations = liquidations_day[liquidations_day.user == liquidated_user]
        user_initial_balance = get_user_balances(
            user=liquidated_user, day=day - timedelta(days=1)
        )
        user_events = get_user_events(user=liquidated_user, day=day)
        add_liquidation_to_user_events(
            user_events=user_events,
            liquidation_events=liquidations,
            liquidation_params=liquidations_params,
        )

        balances = compute_user_balances(
            user_initial_balance=user_initial_balance,
            day_prices=liquidation_day_prices,
            user_events=user_events,
            reserves_data_updated=reserves_data_updated,
            reserves=reserves,
        )
        balances = process_user_balances(
            user=liquidated_user,
            user_balances=balances,
            reserves=reserves,
            pool=pool,
            liquidation_params=liquidations_params,
        )

        probas = compute_liquidation_proba_trajectory(
            user_balances=balances, volatility=volatility, detla_t=delta_t
        )
        hf = compute_health_factor_trajectory(user_balances=balances)
        trajectory = probas.merge(hf, how="left", on=["BlockNumber", "Timestamp"])
        trajectory["user_address"] = liquidated_user
        balances["user_address"] = liquidated_user
        day_trajectories = pd.concat((day_trajectories, trajectory))
        day_user_balances = pd.concat((day_user_balances, balances))

    buffer = io.StringIO()
    day_trajectories.to_csv(buffer, index=False)
    day_str = day.strftime("%Y-%m-%d")
    client_s3.put_object(
        Bucket="projet-datalab-group-jprat",
        Key=output_path
        + f"liquidation_trajectories_snapshot_date={day_str}/liquidation_trajectories.csv",
        Body=buffer.getvalue(),
    )

    buffer = io.StringIO()
    day_user_balances.to_csv(buffer, index=False)
    day_str = day.strftime("%Y-%m-%d")
    client_s3.put_object(
        Bucket="projet-datalab-group-jprat",
        Key=output_path
        + f"liquidation_trajectories_snapshot_date={day_str}/users_balances.csv",
        Body=buffer.getvalue(),
    )

    buffer = io.StringIO()
    volatility.to_csv(buffer, index=False)
    day_str = day.strftime("%Y-%m-%d")
    client_s3.put_object(
        Bucket="projet-datalab-group-jprat",
        Key=output_path
        + f"liquidation_trajectories_snapshot_date={day_str}/volatility.csv",
        Body=buffer.getvalue(),
    )

    day += timedelta(days=1)
