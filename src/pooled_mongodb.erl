-module(pooled_mongodb).

-behaviour(application).

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, StartArgs) ->
    {ok, DefaultArgs} = application:get_env(default),
    NewArgs = case is_map(StartArgs) of
        true ->
            maps:merge(DefaultArgs, StartArgs); % second map takes key president. 
        _ ->
            DefaultArgs
        end, 
    pooled_mongodb_sup:start_link(NewArgs).

-spec stop(term()) -> ok.
stop(_State) ->
    ok.