from datetime import datetime, timedelta
import warnings
import pandas as pd
from pandas import DataFrame
import boto3
import io
import os

warnings.filterwarnings("ignore")

from src.data.liquidations import get_liquidated_users_list
from src.data.prices import get_daily_prices, get_hourly_prices
from src.data.balances import (
    get_user_balances,
    compute_user_balances,
    process_user_balances,
)
from src.data.reserves import get_reserves_data
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
start = datetime(2023, 1, 27)
stop = datetime(2023, 1, 31)
vol_estimation_nb_days = 62
delta_t = 1 / 365


client_s3 = boto3.client(
    "s3",
    endpoint_url = 'https://'+'minio-simple.lab.groupe-genes.fr',
    aws_access_key_id= os.environ["ACCESS_KEY_ID"], 
    aws_secret_access_key= os.environ["SECRET_ACCESS_KEY"], 
)


day = start
while day <= stop:
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

    day_trajectories = DataFrame()
    liquidated_users_list = get_liquidated_users_list(day=day)
    for liquidated in liquidated_users_list:
        liquidation_block = liquidated[0]
        user = liquidated[1]

        balances_before = get_user_balances(user=user, day=day - timedelta(days=1))
        balances_after = get_user_balances(user=user, day=day)
        balances = compute_user_balances(
            balances_before=balances_before,
            balances_after=balances_after,
            prices=liquidation_day_prices,
            liquidation_block=liquidation_block,
        )
        balances = process_user_balances(user_balances=balances, reserves=reserves)

        probas = compute_liquidation_proba_trajectory(
            user_balances=balances, volatility=volatility, detla_t=delta_t
        )
        hf = compute_health_factor_trajectory(user_balances=balances)
        trajectory = probas.merge(hf, how="left", on=["BlockNumber", "Timestamp"])
        trajectory["user_address"] = user
        day_trajectories = pd.concat((day_trajectories, trajectory))

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
    volatility.to_csv(buffer, index=False)
    day_str = day.strftime("%Y-%m-%d")
    client_s3.put_object(
        Bucket="projet-datalab-group-jprat",
        Key=output_path
        + f"liquidation_trajectories_snapshot_date={day_str}/volatility.csv",
        Body=buffer.getvalue(),
    )

    day += timedelta(days=1)
