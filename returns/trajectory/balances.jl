using DataFrames
using Dates


"""
Error thrown by `_find_closest_index` if `asset` is not in `reserves_data_updated`
"""
struct AssetNotReferenced <: Exception
    message::String
end

Base.showerror(io::IO, err::AssetNotReferenced) = print(io, err.message)


"""
Find liquidityIndex or BorrowIndex of a given asset corresponding to the closest block of a given
block number.
# Arguments
- `asset::String`: The smart contract address of the underlying token corresponding to the reserve of interest
- `blockNumber::Int`: The block number at which we want to know the index
- `reserves_data_updated::Matrix`: The table of the reservesDataUpdated() events.
    * column 1: The asset
    * column 2: The block numbers
    * column 3: The liquidity indexes
    * column 4: The variable borrow indexes
- `return_deposit::Bool = false`: Wether to return the liquidityIndex of the variableBorrowIndex
# Returns 
- `Float64` or `missing`: The closest index corresponding to the reserve and blockNumber. If asset not in
reserves_data_updated, return missing
"""
function _find_closest_index(
    asset::String, blockNumber::Int, reserves_data_updated::Matrix; return_deposit::Bool = true
)::Union{Float64, Missing}
    if asset in reserves_data_updated[:, 1]
        id = return_deposit ? 3 : 4
        index = argmin(abs.((reserves_data_updated[:, 1] .== asset) .* reserves_data_updated[:, 2] .- blockNumber))
        return reserves_data_updated[index, id] * 1e-27
    else
        @warn "Asset $asset not found in reserves_data_updated, returning missing instead"
        return missing
        # throw(AssetNotReferenced("Asset $asset not in reserves updates"))
    end
end


"""
Similar to `_find_closest_index` but return index from reserve instread of missing if the
provided asset is not in `reserves_data_updated`
"""
function _find_closest_index(
    asset::String, blockNumber::Int, reserves_data_updated::Matrix, reserves::DataFrame; return_deposit::Bool = true
)::Float64
    if asset in reserves_data_updated[:, 1]
        id = return_deposit ? 3 : 4
        index = argmin(abs.((reserves_data_updated[:, 1] .== asset) .* reserves_data_updated[:, 2] .- blockNumber))
        return reserves_data_updated[index, id] * 1e-27
    else
        @warn "Asset $asset not found in reserves_data_updated, returning index from reserves instead"
        if return_deposit
            parse(Int128, reserves[reserves.underlyingAsset .== asset, "liquidityIndex"][1]) * 1e-27
        end
        return parse(Int128, reserves[reserves.underlyingAsset .== asset, "variableBorrowIndex"][1]) * 1e-27
        # throw(AssetNotReferenced("Asset $asset not in reserves updates"))
    end
end


"""
Give the balance trajectory of a user over a day, without taking into account of the user during that
day (only prices fluctuations matter)
# Arguments
- `user_initial_balance::DataFrame`: Response of '/user-selec-balance' in API call
- `day_prices::DataFrame`: Response of '/prices' in API call
- `reserves_data_updated::DataFrame`: Response of '/events/reservedataupdated' in API call
# Returns
- `DataFrame`: The trajectory of the balances of the user over the given day 
"""
function get_day_trajectory_without_day_events(
    user_initial_balance::DataFrame, day_prices::DataFrame, index_reference::Matrix, reserves::DataFrame
)::DataFrame
    trajectory = select(
        leftjoin(day_prices, user_initial_balance, on = [:UnderlyingToken => :underlyingAsset]),
        Not(:snapshot_block)
    )
    dropmissing!(trajectory, :user_address)
    transform!(trajectory, :BlockNumber => (x -> parse.(Int, x)) => :BlockNumber)
    transform!(
        trajectory, 
        [:BlockNumber, :UnderlyingToken] => ByRow(
            (block, asset) -> _find_closest_index(asset, block, index_reference, reserves)
        ) => :liquidityIndex,
        [:BlockNumber, :UnderlyingToken] => ByRow(
            (block, asset) -> _find_closest_index(asset, block, index_reference, reserves; return_deposit = false)
        ) => :variableBorrowIndex,
    )
    select!(
        trajectory,
        :user_address,
        :BlockNumber,
        :Timestamp,
        :UnderlyingToken,
        :name,
        :Price,
        [:scaledATokenBalance, :decimals, :liquidityIndex] => ((b, d, i) -> b ./ 10 .^ d .* i) => :currentATokenBalance,
        [:scaledVariableDebt, :decimals, :variableBorrowIndex] => ((b, d, i) -> b ./ 10 .^ d .* i) => :currentVariableDebt,
    )
    return trajectory
end


"""
Include user actions during a day to get the balances trajectory of a user during that day.
# Arguments
- `trajectory_without_events::DataFrame`: Output from `get_day_trajectory_without_day_events` function
- `user_events::DataFrame`: Output from `get_user_events_during_day` and `add_liquidation_to_user_events` functions
# Returns
- `DataFrame`: The user balances trajectory over a day, taking into account the actions of the user during the day
"""
function get_day_trajectory!(trajectory_without_events::DataFrame, user_events::DataFrame)::DataFrame
    for event in eachrow(user_events)
        mask = (
            (trajectory_without_events.BlockNumber .>= event["blockNumber"])
            .& (trajectory_without_events.UnderlyingToken .== event["reserve"])
        )
        if event["action"] in ["supply", "balancetransfer_receive"]
            trajectory_without_events[mask, :currentATokenBalance] .+= event["amount"]
        elseif event["action"] == "borrow"
            trajectory_without_events[mask, :currentVariableDebt] .+= event["amount"]
        elseif event["action"] in ["withdraw", "balancetransfer_send"]
            trajectory_without_events[mask, :currentATokenBalance] .-= event["amount"]
        elseif event["action"] == "repay"
            trajectory_without_events[mask, :currentVariableDebt] .-= event["amount"]
        end
    end
    select!(
        trajectory_without_events,
        :user_address,
        :BlockNumber,
        :Timestamp,
        :UnderlyingToken,
        :name,
        [:currentATokenBalance, :Price] => ((b, p) -> b .* p * 1e-8) => :currentATokenBalanceUSD,
        [:currentVariableDebt, :Price] => ((b, p) -> b .* p * 1e-8) => :currentVariableDebtUSD,
    )
end
