using DataFrames


"""
Compute the health factor trajectory of a user during a day.
# Arguments
- `user_day_trajectory::DataFrame`: Output from `get_day_trajectory!` function
- `reserves::DataFrame`: The daily reserves data
"""
function compute_health_factor(user_day_trajectory::DataFrame, reserves::DataFrame)::DataFrame
    # Merge user trajectory with reserves to get liq. threshold
    trajectory = leftjoin(
        user_day_trajectory,
        select(reserves, :underlyingAsset, :reserveLiquidationThreshold),
        on = [:UnderlyingToken => :underlyingAsset]
    )
    # compute numerator and denominator of hf
    transform!(
        trajectory,
        [:currentATokenBalanceUSD, :reserveLiquidationThreshold] => ((b, lt) -> b .* lt * 1e-4) => :numerator
    )
    trajectory = combine(
        groupby(trajectory, [:user_address, :BlockNumber, :Timestamp]),
        :numerator => sum => :numerator,
        :currentVariableDebtUSD => sum => :denominator
    )
    # compute hf
    transform!(trajectory, [:numerator, :denominator] => ((x, y) -> x ./ y) => :hf)
    return trajectory
end


"""
Find the latest block before liquidation with an health factor above 1.
# Arguments
- `user_hf_trajectory::DataFrame`: Output from `compute_health_factor` function
- `liquidation_block::Int`: The block at which the liquidation occured
"""
function find_hf_drops(user_hf_trajectory::DataFrame, liquidation_blocks::Vector{Int})::Vector{Int}
    if length(liquidation_blocks) > 1
        n = length(liquidation_blocks)
        user = user_hf_trajectory.user_address[1]
        # @info "User $user was liquidated $n times that day"
    end
    drop_blocks::Vector{Int} = Int[]
    for lb in liquidation_blocks
        mask = (user_hf_trajectory.BlockNumber .< lb) .& (user_hf_trajectory.hf .>= 1)
        if nrow(user_hf_trajectory[mask, :]) == 0
            user = user_hf_trajectory.user_address[1]
            @warn "user: $user, liquidation block $lb : could not find a block before liquidation with hf > 1, skipping this record"
        else
            push!(drop_blocks, maximum(user_hf_trajectory[mask, :].BlockNumber))
        end
    end
    return unique(drop_blocks)
end