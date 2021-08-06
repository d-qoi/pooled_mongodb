-module(pooled_mongodb).

-export([initialize/1]).

-spec initialize(application:start_type()) -> {ok, pid()} | {error, term()}.
initialize(PoolArgs) ->
    {ok, DefaultArgs} = application:get_env(default),
    NewArgs = case is_map(PoolArgs) of
        true ->
            maps:merge(DefaultArgs, PoolArgs); % second map takes key president. 
        _ ->
            DefaultArgs
        end, 
    pooled_mongodb_sup:start_link(NewArgs).
