%%%----------------------------------------------------------------------
%%% Created	: 2014-5-6
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_ctl).

-include("elog.hrl").

-compile(export_all).

lookup_pid(Key) ->
	chash_pg:get_pid(ertdb, list_to_binary(Key)).
	
lookup_pid2(Key) ->
	process_info(chash_pg:get_pid(ertdb, list_to_binary(Key)), [registered_name]).	

lookup(Key) ->
	ertdb:lookup(list_to_binary(Key)).
	
lookup_his(Key) ->	
	ertdb:lookup_his(list_to_binary(Key)).

fetch(Key) ->
	ertdb:fetch(list_to_binary(Key)).
	
lookup_config(No) ->	
	ertdb:lookup_info(ertdb:name(No)).
	
lookup_current(No) ->	
	ertdb_store_current:lookup_info(ertdb_store_current:name(No)).		
	
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
    process_info(whereis(list_to_atom(Process)), [memory, message_queue_len,heap_size,total_heap_size]).	

process2(Process) ->
    process_info(whereis(list_to_atom(Process)), [messages]).	
	
memory() ->
	erlang:memory().	
	
state(Module) ->
	sys:get_status(list_to_atom(Module)).	
	
	
	