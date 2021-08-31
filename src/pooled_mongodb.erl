-module(pooled_mongodb).

-export([initialize/1]).

-export([insert/1,
        update/1,
        delete/1,
        find/1,
        find_one/1,
        count/1,
        command/1,
        bulk_return_last/1]).

-type cursor() :: pid().
-type insert_response() :: {{boolean(), map()}, bson:document()} | {{boolean(), map()}, map()} |  {{boolean(), map()}, list()}.
-type update_response() :: {boolean(), map()}.
-type delete_response() :: {boolean(), map()}.
-type find_response() :: {ok, cursor()} | [].
-type find_one_response() :: map() | undefined.
-type count_response() :: integer().
-type command_response() :: {boolean(), map()} | {ok, cursor()}.

-type all_responses() :: insert_response() | update_response() | delete_response() | 
                         find_response() | find_one_response() | count_response() | command_response().

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


%% @doc Insert one document or multiple documents into a colleciton.
%% params:
%%  collection - collection()
%%  doc - bson:document() or list
%%  database - insert in this database (optional)
%%  write_concern - bson:document() (optional)
-spec insert(map()) -> insert_response().
insert(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {insert, Cmd})
    end).

%% @doc Replace the document matching criteria entirely with the new Document.
%% params:
%%  collection - collection()
%%  selector - selector()
%%  doc - bson:document() or list
%%  database - insert in this database (optional)
%%  upsert - boolean() do upsert (optional)
%%  multi - boolean() multiupdate (optional)
%%  write_concern - bson:document() (optional)
-spec update(map()) -> update_response().
update(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {update, Cmd})
    end).

%% @doc Delete selected documents
%% params:
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  num - int() number of documents (optional). 0 = all (default)
%%  write_concern - bson:document() (optional)
-spec delete(map()) -> delete_response().
delete(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {delete, Cmd})
    end).

%% @doc Return projection of selected documents.
%% params:
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  projector - bson:document() (optional)
%%  skip - int() (optional)
%%  readopts - bson:document() (optional)
-spec find(map()) -> find_response().
find(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {find, Cmd})
    end).

%% @doc Return projection of selected documents.
%% params:
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  projector - bson:document() (optional)
%%  skip - int() (optional)
%%  readopts - bson:document() (optional)
-spec find_one(map()) -> find_one_response().
find_one(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {find_one, Cmd})
    end).


%% @doc Return projection of selected documents.
%% params:
%%  collection - collection()
%%  selector - selector()
%%  database - insert in this database (optional)
%%  limit - int() (optional). 0 - no limit
%%  readopts - bson:document() (optional)
-spec count(map()) -> count_response().
count(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {count, Cmd})
    end).

-spec command(map()) -> command_response().
command(Cmd) ->
    poolboy:transaction(mongos, fun(Worker) ->
        gen_server:call(Worker, {command, Cmd})
    end).

-spec bulk_return_helper(Worker :: pid(), Commands :: [{atom(), map()}]) -> all_responses().
bulk_return_helper(Worker, [Head | []]) ->
    gen_server:call(Worker, Head);
bulk_return_helper(Worker, [Head | Tail]) ->
    gen_server:call(Worker, Head),
    bulk_return_helper(Worker, Tail).

-spec bulk_return_last([{atom(), map()}]) -> all_responses().
bulk_return_last(Commands) ->
    poolboy:transaction(mongos, fun(Worker) ->
        bulk_return_helper(Worker, Commands)
    end).