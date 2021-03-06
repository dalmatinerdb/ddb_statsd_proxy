%%%-------------------------------------------------------------------
%% @doc ddb_statsd_proxy top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module('ddb_statsd_proxy_sup').

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    C = ?CHILD(ddb_statsd_udp, worker),
    {ok, { {one_for_all, 0, 1}, [C]} }.

%%====================================================================
%% Internal functions
%%====================================================================
