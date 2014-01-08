%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 10, Type, [I]}).
-define(CHILD(I, Args, Type), {I, {I, start_link, Args}, permanent, 10, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
	
init([]) ->
    
	Ertdb = ?CHILD(ertdb, worker), 
	%% Httpd config
	{ok, HttpdConf} = application:get_env(httpd), 
	%% Httpd 
    Httpd = {ertdb_httpd, {ertdb_httpd, start, [HttpdConf]},
           permanent, 10, worker, [ertdb_httpd]},

	%% Socket config
	{ok, SocketConf} = application:get_env(socket), 
	%% Socket
    Socket = {ertdb_socket, {ertdb_socket, start, [SocketConf]},
           permanent, 10, worker, [ertdb_socket]},
		   
	Journal = ?CHILD(ertdb_journal, worker),	   
	
	CurStore = ?CHILD(ertdb_store_current, worker),
	HisStore = ?CHILD(ertdb_store_history, worker),
		   
	{ok, {{one_for_one, 5, 10}, [Ertdb, Socket, Journal, CurStore, HisStore]}}.
	