%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2015, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 20 Oct 2015 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ddb_statsd_udp).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {udp_port, udp,
                ddb_host, ddb_port, ddb_bucket, ddb}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    Port = application:get_env(ddb_statsd_proxy, port, 8125),
    {DDBHost, DDBPort} = application:get_env(ddb_statsd_proxy, ddb,
                                             {"localhost", 5555}),
    BucketS = application:get_env(ddb_statsd_proxy, ddb_bucket, "statsd"),
    Bucket = list_to_binary(BucketS),
    process_flag(trap_exit, true),
    {ok, #state{udp_port = Port, ddb_host = DDBHost, ddb_port = DDBPort,
                ddb_bucket = Bucket}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(timeout, State = #state{udp_port = Port,
                                    ddb_port = DDBPort,
                                    ddb_host = DDBHost,
                                    ddb_bucket = Bucket}) ->
    {ok, C} = gen_udp:open(Port, [binary, {active, once}]),
    D = case ddb_tcp:connect(DDBHost, DDBPort) of
            {ok, Dx} ->
                Dx;
            {error, _, Dx} ->
                Dx
        end,
    D1 = case ddb_tcp:stream_mode(Bucket, 2, D) of
             {ok, D1x} ->
                 D1x;
             {error, _, D1x} ->
                 D1x
         end,
    {noreply, State#state{udp = C, ddb = D1}};

handle_info({udp, C, _IP, _Port, Msg}, State = #state{udp = C}) ->
    T = erlang:system_time(seconds),
    inet:setopts(C, [{active, once}]),
    decode(T, Msg),
    {noreply, State};

handle_info(Info, State) ->
    io:format("~p~n", [Info]),
    {noreply, State}.

decode(T, M) ->
    case ddb_statsd_proxy:decode(M) of
        {incomplete, M} ->
            io:format("Error: ~p~n", [M]);
        {C, <<>>} ->
            handle_v(T, C);
        {C, R}->
            handle_v(T, C),
            decode(T, R)
    end.

handle_v(T, {gauge, M, V}) ->
    io:format("[gauge] ~p@~p~n", [{M, V}, T]);
handle_v(_, C) ->
    io:format("[unsupported] ~p~n", [C]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{udp = undefined, ddb = undefined}) ->
    ok;
terminate(_Reason, #state{udp = C, ddb = D}) ->
    gen_udp:close(C),
    ddb_tcp:close(D),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
