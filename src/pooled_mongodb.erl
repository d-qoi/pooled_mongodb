-module(pooled_mongodb).

-export([initialize/1]).

-spec initialize(application:start_type()) -> {ok, pid()} | {error, term()}.
initialize(PoolArgs) ->
    DefaultArgs = #{name => mongos, 
                    pool_args => [{name, {local, mongos}}, {size, 3},{max_overflow, 10},{worker_module, pooled_mongodb_worker}],
                    mongo_args => [{database, <<"pooled_mongodb">>}]},
    NewArgs = case is_map(PoolArgs) of
        true ->
            maps:merge(DefaultArgs, PoolArgs); % second map takes key president. 
        _ ->
            DefaultArgs
        end, 
    pooled_mongodb_sup:start_link(NewArgs).
