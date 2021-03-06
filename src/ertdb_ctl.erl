%%%----------------------------------------------------------------------
%%% Created	: 2014-5-6
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_ctl).

-include("elog.hrl").

-compile(export_all).

lookup_pid(Key) ->
	node(chash_pg:get_pid(ertdb, list_to_binary(Key))).
	
lookup_pid2(Key) ->
	process_info(chash_pg:get_pid(ertdb, list_to_binary(Key)), [registered_name]).	

lookup(Key) ->
	ertdb:lookup(list_to_binary(Key)).
	
lookup_his(Key) ->	
	ertdb:lookup_his(list_to_binary(Key)).
	
lookup_server(Process) ->
	ertdb_server:lookup(whereis(list_to_atom(Process))).	
	
lookup_info(Ets, Key) ->
	ets:lookup(list_to_atom(Ets), list_to_binary(Key)).	

lookup_ckey(Key) ->
	ets:lookup(ckey, list_to_binary(Key)).

fetch(Key) ->
	ertdb:fetch(list_to_binary(Key)).
		
	
cluster_info() ->
    Nodes = [node()|nodes()],
    ?PRINT("cluster nodes: ~p~n", [Nodes]).

cluster(Node) ->
	case net_adm:ping(list_to_atom(Node)) of
	pong ->
		?PRINT("cluster with ~p successfully.~n", [Node]);
	pang ->
        ?PRINT("failed to cluster with ~p~n", [Node])
	end.

sockets() ->
    ActiveSockets = mochiweb_socket_server:get(ertdb_socket, active_sockets),
    ?PRINT("Total Client Sockets: ~p~n", [ActiveSockets]).

%Type: ertdb | jour | hist
process_all(Type) ->
    Infos = lists:flatten(ertdb:info(Type)),
    [[Name, Info] || {Name, Info} <- Infos].	

%Type : ertdb_rttb | ertdb_hist | ertdb_rtk_config
ets_info(Type) ->
    Tabs = ets:all(),
    ErrdbTabs = lists:filter(fun(Tab) -> 
        if
        is_atom(Tab) ->
            lists:prefix(Type, atom_to_list(Tab));
        true ->
            false
        end
    end, Tabs),
    [[Tab, ets:info(Tab)] || Tab <- ErrdbTabs].

%% sysinfo
status() ->
    {InternalStatus, ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p. Status: ~p~n",
              [node(), InternalStatus, ProvidedStatus]),
    case lists:keysearch(ertdb, 1, application:which_applications()) of
    false ->
        ?PRINT("ertdb is not running~n", []);
    {value, Version} ->
        ?PRINT("ertdb ~p is running~n", [Version])
    end.
	
process(Process) ->
    process_info(whereis(list_to_atom(Process)), [memory, message_queue_len,heap_size,total_heap_size, reductions]).	

process2(Process) ->
    process_info(whereis(list_to_atom(Process)), [messages]).	
	

%% systerm
process_count() ->	
	erlang:system_info(process_count).	
	
	
memory() ->
	erlang:memory().	
	
state(Module) ->
	sys:get_status(list_to_atom(Module)).	
	
	
%% test
test(Count, Step) ->
	spawn(fun() ->
		ertdb_test:go(list_to_integer(Count), list_to_integer(Step))
	end).
	
test_config(Count, Step) ->
	spawn(fun() ->
		ertdb_test:config(list_to_integer(Count), list_to_integer(Step))
	end).	

test_stop() ->
	ertdb_test:stop().			

	
	
	