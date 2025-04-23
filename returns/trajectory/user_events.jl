using DataFrames
using Dates

include("../utils/request.jl")


function get_user_events_during_day(user::String, day::Date, reserves::DataFrame)::DataFrame
    output::DataFrame = DataFrame(
        blockNumber = Int[],
        reserve = String[],
        amount = Int[],
        action = String[],
    )
    events_list = ["supply", "borrow", "withdraw", "repay"]
    for event in events_list
        result = execute_api_query_as_dc(
            "https://aavedata.lab.groupe-genes.fr/events/$event", 
            ["date" => to_query_format(day)],
            verify = false
        )
        if event in ["withdraw", "repay"]
            usercol = "user"
        else
            usercol = "onBehalfOf"
        end
        user_events = [ev for ev in result if ev[usercol] == user]
        if length(user_events) > 0
            user_events_df = select(DataFrame(user_events), :blockNumber, :reserve, :amount)
            user_events_df[:, :action] .= event
            output = vcat(output, user_events_df)
        end
    end

    # Add balancetransfer
    result = execute_api_query_as_dc(
        "https://aavedata.lab.groupe-genes.fr/events/balancetransfer", 
        ["date" => to_query_format(day)],
        verify = false
    )
    # Send
    user_events = [ev for ev in result if ev["from"] == user]
    if length(user_events) > 0
        user_transfer_send = select(DataFrame(user_events), :blockNumber, :reserve, :amount)
        user_transfer_send[:, :action] .= "balancetransfer_send"
        output = vcat(output, user_transfer_send)
    end
    # Receive
    user_events = [ev for ev in result if ev["to"] == user]
    if length(user_events) > 0
        user_transfer_rec = select(DataFrame(user_events), :blockNumber, :reserve, :amount)
        user_transfer_rec[:, :action] .= "balancetransfer_receive"
        output = vcat(output, user_transfer_rec)
    end

    # Join with reserves data to get reserve decimals and
    # liquidityIndex for aToken transfers which are scaled
    output = leftjoin(
        output, 
        select(
            reserves,
            :underlyingAsset,
            :decimals,
            :liquidityIndex => (x -> parse.(Int128, x) .* 1e-27) => :liquidityIndex
        ),
        on = [:reserve => :underlyingAsset]
    )
    select!(
        output,
        :blockNumber,
        :reserve,
        :action,
        [:action, :amount, :decimals, :liquidityIndex] => (
            (act, a, dec, li) -> (
                act in ["balancetransfer_send", "balancetransfer_receive"]
            ) ? a .* li ./ (10 .^ dec) : a ./ (10 .^ dec)
        ) => :amount
    )
    return output
end


function add_liquidation_to_user_events!(
    user_events_day::DataFrame, user_liquidations_day::SubDataFrame, reserves::DataFrame
)
    seized_collateral = select(user_liquidations_day, :blockNumber, :collateralAsset, :liquidatedCollateralAmount)
    rename!(seized_collateral, :collateralAsset => :reserve, :liquidatedCollateralAmount => :amount)
    seized_collateral.action .= "withdraw"
    repaid_debt = select(user_liquidations_day, :blockNumber, :debtAsset, :debtToCover)
    rename!(repaid_debt, :debtAsset => :reserve, :debtToCover => :amount)
    repaid_debt.action .= "repay"

    extra_events = vcat(seized_collateral, repaid_debt)

    leftjoin!(
        extra_events, 
        select(
            reserves,
            :underlyingAsset,
            :decimals,
        ),
        on = [:reserve => :underlyingAsset]
    )
    extra_events.amount = extra_events.amount ./ 10 .^ extra_events.decimals
    select!(extra_events, Not(:decimals))

    append!(user_events_day, extra_events)    
end