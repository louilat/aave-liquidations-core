include("trajectory/Trajectory.jl")

using Dates
using .Trajectory
using CSV
using DataFrames

start = Date(2024, 1, 1)
stop = Date(2024, 12, 31)
# day_data = DayLevelData(day; verify = false)

liq_blocks = find_hf_drop_blocks(start, stop)

df = DataFrame(blockNumber = liq_blocks, liquidation = true)
CSV.write("returns/outputs/liquidation_blocks.csv", df)
println(liq_blocks)
