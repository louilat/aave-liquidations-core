module Trajectory

include("../utils/request.jl")
include("../utils/days_range.jl")
include("user_events.jl")
include("balances.jl")
include("health_factor.jl")

using Dates
using DataFrames


export DayLevelData
export get_hf_trajectory
export find_hf_drop_blocks


struct DayLevelData
    day::Date
    reserves::DataFrame
    day_prices::DataFrame
    reserves_data_updated::DataFrame
    liquidations::DataFrame
end


function DayLevelData(day::Date; verify::Bool = true)::DayLevelData
    day_str = to_query_format(day)
    reserves = execute_api_query_as_df(
        "https://aavedata.lab.groupe-genes.fr/reserves", 
        ["date" => day_str];
        verify = verify,
    )
    day_prices = execute_api_query_as_df(
        "https://aavedata.lab.groupe-genes.fr/prices",
        ["date" => day_str];
        verify = verify,
    )
    reserves_data_updated = execute_api_query_as_df(
        "https://aavedata.lab.groupe-genes.fr/events/reservedataupdated",
        ["date" => day_str];
        verify = verify,
    )
    liquidations = execute_api_query_as_df(
        "https://aavedata.lab.groupe-genes.fr/events/liquidation",
        ["date" => day_str];
        verify = verify,
    )

    return DayLevelData(
        day, reserves, day_prices, reserves_data_updated, liquidations
    )
end


function get_hf_trajectory(user::String, day_data::DayLevelData)::Union{DataFrame, Missing}
    # Extract user level data: initial_balance, events during day
    init_balance = execute_api_query_as_df(
        "https://aavedata.lab.groupe-genes.fr/user-selec-balances",
        [
            "date" => to_query_format(day_data.day - Day(1)), #to_query_format(day_data.day - Day(1))
            "user" => user
        ];
        verify = false,
    )

    if ismissing(init_balance)
        day = day_data.day
        @warn "Initial balance not found for user $user and day $day, returning missing instead of hf_trajectory"
        return missing
    end

    user_events = get_user_events_during_day(user, day_data.day, day_data.reserves)
    add_liquidation_to_user_events!(
        user_events,
        view(day_data.liquidations, day_data.liquidations.user .== user, :),
        day_data.reserves,
    )

    index_reference = Matrix(select(
        day_data.reserves_data_updated,
        :reserve,
        :blockNumber,
        :liquidityIndex => (x -> parse.(Int128, x)) => :liquidityIndex,
        :variableBorrowIndex => (x -> parse.(Int128, x)) => :variableBorrowIndex,
    ))

    trajectory = get_day_trajectory_without_day_events(
        init_balance, day_data.day_prices, index_reference, day_data.reserves
    )

    get_day_trajectory!(trajectory, user_events)

    return compute_health_factor(trajectory, day_data.reserves)
end


function find_hf_drop_blocks(day_data::DayLevelData)::Vector{Int}
    users_list = unique(day_data.liquidations.user)
    n = length(users_list)
    day = day_data.day

    drop_blocks = Vector{Vector{Int}}(undef, n)
    @info "Found $n users for day $day"
    Threads.@threads for i in eachindex(users_list)
        user = users_list[i]
        @info "[$i / $n] Treating day $day, user: $user"
        hf_trajectory = get_hf_trajectory(user, day_data)
        
        if ismissing(hf_trajectory)
            @warn "Got missing instead of hf_trajectory for user $user and day $day, skipping this user"
            drop_blocks[i] = Int[]
            continue
        end

        drop_blocks[i] = find_hf_drops(
            hf_trajectory, day_data.liquidations[day_data.liquidations.user .== user, :].blockNumber,
        )
    end
    return unique(vcat(drop_blocks...))
end


function find_hf_drop_blocks(start::Date, stop::Date)::Vector{Int}
    @assert start <= stop "start day should be before stop day"
    
    days_list = range(start, stop)
    liq_blocks = Vector{Vector{Int}}(undef, length(days_list))

    Threads.@threads for i in eachindex(days_list)
        day = days_list[i]
        day_data = DayLevelData(day; verify = false)
        if nrow(day_data.liquidations) == 0
            @info "No liquidations for day $day"
            liq_blocks[i] = Int[]
            continue
        end
        liq_blocks_day = find_hf_drop_blocks(day_data)
        liq_blocks[i] = liq_blocks_day
    end
    return unique(vcat(liq_blocks...))
end

end # End of module
