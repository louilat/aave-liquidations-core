using Dates
using DataFrames
using HTTP
using JSON3


to_query_format = (dt::Date) -> string(dt)[1:4] * "-" * monthname(dt)[1:3] * "-" * string(dt)[end-1:end]


function execute_api_query_as_df(url::String, query::Vector{Pair{String, String}}; verify::Bool = true)::Union{DataFrame, Missing}
    resp = HTTP.get(url; query = query, require_ssl_verification = verify)
    if resp.body != b"null"
        return DataFrame(JSON3.read(resp.body))
    end
    @warn "API return null body for url: $url, query = $query, returning missing"
    return missing
end


function execute_api_query_as_dc(url::String, query::Vector{Pair{String, String}}; verify::Bool = true)::Union{JSON3.Array, Missing}
    resp = HTTP.get(url; query = query, require_ssl_verification = verify)
    if resp.body != b"null"
        return JSON3.read(resp.body)
    end
    @warn "API return null body for url: $url, query = $query, returning missing"
    return missing
end
