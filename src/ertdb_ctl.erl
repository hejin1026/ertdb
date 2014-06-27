%%%----------------------------------------------------------------------
%%% Created	: 2014-5-6
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_ctl).

-include("elog.hrl").

-compile(export_all).

lookup(Key) ->
	ertdb:lookup(list_to_binary(Key)).
	
lookup_his(Key) ->	
	ertdb:lookup_his(list_to_binary(Key)).

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

status() ->
    Infos = lists:flatten(errdb:info()),
    [?PRINT("process ~p: ~n~p~n", [Name, Info]) || {Name, Info} <- Infos],
    Tabs = ets:all(),
    ErrdbTabs = lists:filter(fun(Tab) -> 
        if
        is_atom(Tab) ->
            lists:prefix("ertdb", atom_to_list(Tab));
        true ->
            false
        end
    end, Tabs),
    [?PRINT("table ~p:~n~p~n", [Tab, ets:info(Tab)]) || Tab <- ErrdbTabs],
    {InternalStatus, ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p. Status: ~p~n",
              [node(), InternalStatus, ProvidedStatus]),
    case lists:keysearch(ertdb, 1, application:which_applications()) of
    false ->
        ?PRINT("ertdb is not running~n", []);
    {value, Version} ->
        ?PRINT("ertdb ~p is running~n", [Version])
    end.
	