-module(pooled_mongodb_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link(map()) -> pid() | term().
start_link(Args) -> supervisor:start_link({local, ?SERVER}, ?MODULE, Args).

-spec init(term()) -> {ok, {supervisor:sup_flags(), supervisor:child_spec()}}.
init(#{name := Name, pool_args := PoolArgs, mongo_args := MongoArgs}) ->
    SupFlags = #{strategy => one_for_one,
                intensity => 0,
                period => 1},
    ChildSpecs = poolboy:child_spec(Name, PoolArgs, MongoArgs),
    {ok, {SupFlags, [ChildSpecs]}}.

