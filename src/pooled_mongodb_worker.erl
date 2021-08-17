-module(pooled_mongodb_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-type arg() :: {database, database()}
| {login, binary()}
| {password, binary()}
| {w_mode, write_mode()}
| {r_mode, read_mode()}
| {host, list()}
| {port, integer()}
| {ssl, boolean()}
| {ssl_opts, proplists:proplist()}
| {register, atom() | fun()}.
-type database() :: binary() | atom().
-type write_mode() :: unsafe | safe | {safe, bson:document()}.
-type read_mode() :: master | slave_ok.

-spec start_link(arg()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec init(arg()) -> {ok, map()}.
init(Args) ->
    {ok, ConnPid} = mc_worker_api:connect(Args),
    {ok, #{pid=>ConnPid}}.

-spec handle_call({atom(), map()}, term(), map()) -> {reply, ok | {ok, term()} | {ok, term(), term()}, map()}.
handle_call({insert, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:insert(Cmd#{connection => ConnPid}),
    {reply, Response, State};

handle_call({update, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:update(Cmd#{connection => ConnPid}),
    {reply, Response, State};

handle_call({delete, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:delete(Cmd#{connection => ConnPid}),
    {reply, Response, State};

handle_call({find, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:find(Cmd#{connection => ConnPid}),
    {reply, Response, State};

handle_call({find_one, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:find_one(Cmd#{connection => ConnPid}),
    {reply, Response, State};

handle_call({count, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:count(Cmd#{connection => ConnPid}),
    {reply, Response, State};

handle_call({command, Cmd}, _From, State=#{pid := ConnPid}) ->
    Response = mc_worker_api:command(ConnPid, Cmd),
    {reply, Response, State};

handle_call(_Arg, _From, State) ->
    {reply, ok, State}.

-spec handle_cast({atom(), map()}, map()) -> {noreply, map()}.
handle_cast({insert, Cmd}, State=#{pid := ConnPid}) ->
    mc_worker_api:insert(Cmd#{connection => ConnPid}),
    {noreply, State};

handle_cast({update, Cmd}, State=#{pid := ConnPid}) ->
    mc_worker_api:update(Cmd#{connection => ConnPid}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), map()) -> ok.
terminate(_Reason, #{pid := Pid}) ->
    mc_worker_api:disconnect(Pid),
    ok.