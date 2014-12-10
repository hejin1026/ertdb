%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Helper macro for declaring children of supervisor
-define(worker(I), {I, {I, start_link, []}, permanent, 10, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
	
init([]) ->
	Server = ?worker(ertdb_server),
	
	Pool = 
		case application:get_env(pool_size) of
			{ok, schedulers} -> erlang:system_info(schedulers);
			{ok, Val} when is_integer(Val) -> Val;
			undefined -> erlang:system_info(schedulers)
		end,
    Ertdbs = [{ertdb:name(Id), {ertdb, start_link, [Id]},
	   permanent, 10, worker, [ertdb]} || Id <- lists:seq(1, Pool)],
    
	%% Httpd config
	{ok, HttpdConf} = application:get_env(httpd), 
	%% Httpd 
    Httpd = {ertdb_httpd, {ertdb_httpd, start, [HttpdConf]},
	   permanent, 10, worker, [ertdb_httpd]},

	Pg = ?worker(chash_pg),	   

	%% Socket config
	{ok, SocketConf} = application:get_env(socket), 
	%% Socket
    % Socket = {ertdb_socket, {ertdb_socket, start, [SocketConf]},
% 	   permanent, 10, worker, [ertdb_socket]},

	Socket = {ertdb_esockd, {ertdb_esockd, start, [SocketConf]},
	   permanent, 10, worker, [ertdb_esockd]},
	   
	Monitor = ?worker(ertdb_monitor),
		   
	% Journal = ?CHILD(ertdb_journal, worker),	   	%
	% 
	% CurStore = ?CHILD(ertdb_store_current, worker),
	% HisStore = ?CHILD(ertdb_store_history, worker),
	Test = case application:get_env(test) of
		{ok, true} ->[?worker(ertdb_test)];
		_ -> []
	end,		
		   
	{ok, {{one_for_one, 5, 10}, Test ++ [Server, Socket, Httpd, Pg, Monitor|Ertdbs]}}.
	