using Dates

function range(start::Date, stop::Date)::Vector{Date}
    r = []
    d = start
    while d <= stop
        push!(r, d)
        d += Day(1)
    end
    return r
end