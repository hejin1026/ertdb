%%%----------------------------------------------------------------------
%%% File    : chash_pg.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : consistent hash process group to replace pg2.
%%% Created : 24 Dec 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(chash_pg).

-include("elog.hrl").

-export([create/1, delete/1, join/2, join/3, join/4, leave/2]).

-export([which_groups/0, 
         get_local_vnodes/1, 
         get_vnodes/1, 
         get_vnode/2,
         get_pids/1,
         get_pid/2]).

-export([start/0, start_link/0]).

-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2, 
         terminate/2, 
         code_change/3]).


-record(state, {links = []}).

-define(VNODES_NUM, 40).

%%%-----------------------------------------------------------------
%%% This module implements distributed process groups based on consistent 
%%% hash algorithm, in a different way than the module pg2.  
%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
    ensure_started().

which_groups() ->
    ensure_started(),
    ets:filter(chash_pg_table,
	       fun([{{vnodes, Group}, _}]) ->
		       {true, Group};
		  (_) ->
		       false
	       end,
	       []).

create(Name) ->
    ensure_started(),
    case ets:lookup(chash_pg_table, {local_vnodes, Name}) of
	[] ->
	    global:trans({{?MODULE, Name}, self()},
			 fun() ->
				 gen_server:multi_call(?MODULE, {create, Name})
			 end);
	_ ->
	    ok
    end,
    ok.

delete(Name) ->
    ensure_started(),
    global:trans({{?MODULE, Name}, self()},
		 fun() ->
			 gen_server:multi_call(?MODULE, {delete, Name})
		 end),
    ok.

join(Name, Pid) when is_pid(Pid) ->
    join(Name, Pid, ?VNODES_NUM).

join(Name, Pid, VNodeNum) when is_pid(Pid) ->
    join(Name, Pid, undefined, VNodeNum).

join(Name, Pid, PName, VNodeNum) 
    when is_pid(Pid) and is_atom(PName) ->
    ensure_started(),
    case ets:lookup(chash_pg_table, {vnodes, Name}) of
	[] ->
	    {error, {no_such_group, Name}};
	_ ->
	    global:trans({{?MODULE, Name}, self()},
			 fun() ->
				 gen_server:multi_call(?MODULE,
						       {join, Name, Pid, PName, VNodeNum})
			 end),
	    ok
    end.

leave(Name, Pid) when is_pid(Pid) ->
    ensure_started(),
    case ets:lookup(chash_pg_table, {vnodes, Name}) of
        [] ->
            {error, {no_such_group, Name}};
        _ ->
            global:trans({{?MODULE, Name}, self()},
                         fun() ->
                                 gen_server:multi_call(?MODULE,
                                                       {leave, Name, Pid})
                         end),
            ok
    end.

get_local_vnodes(Name) ->
    ensure_started(),
    case ets:lookup(chash_pg_table, {local_vnodes, Name}) of
	[] -> 
        {error, {no_such_group, Name}};
	[{_, VNodes}] -> 
        VNodes
    end.
    
get_vnodes(Name) ->
    ensure_started(),
    case ets:lookup(chash_pg_table, {vnodes, Name}) of
	[] -> 
        {error, {no_such_group, Name}};
	[{_, VNodes}] -> 
        VNodes
    end.

get_vnode(Name, Key) ->
    case get_vnodes(Name) of
    [] -> 
        {error, {no_process, Name}};
    [H|_] = VNodes ->
        Hash = chash:hash(extbif:to_list(Key)),
        case find_vnode(Hash, VNodes) of
        {ok, VNode} ->
            VNode;
        false ->
            H
        end
    end.

get_pids(Name) ->
    VNodes = get_vnodes(Name),
    Pids = [Pid || {_, Pid, _} <- VNodes], 
    lists:usort(Pids).

get_pid(Name, Key) ->
    case get_vnode(Name, extbif:to_list(Key)) of
    {_Key, Pid, _Vid} -> Pid;
    {error, Error} -> {error, Error}
    end.

%%%-----------------------------------------------------------------
%%% Callback functions from gen_server
%%%-----------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    Ns = nodes(),
    net_kernel:monitor_nodes(true),
    lists:foreach(fun(N) ->
			  {?MODULE, N} ! {new_chash_pg, node()},
			  self() ! {nodeup, N}
		  end, Ns),
    % chash_pg_table keeps track of all vnodesin a group
    ets:new(chash_pg_table, [set, protected, named_table]),
    {ok, #state{}}.

handle_call({create, Name}, _From, S) ->
    case ets:lookup(chash_pg_table, {local_vnodes, Name}) of
	[] ->
	    ets:insert(chash_pg_table, {{local_vnodes, Name}, []}),
	    ets:insert(chash_pg_table, {{vnodes, Name}, []});
	_ ->
	    ok
    end,
    {reply, ok, S};

handle_call({join, Name, Pid, PName, VNodeNum}, _From, S) ->
	?INFO("join pg:~p", [{Name, Pid, PName}]),
    case ets:lookup(chash_pg_table, {vnodes, Name}) of
	[{_, VNodes}] ->
        NewVNodes = create_vnodes(Pid, PName, VNodeNum),
	    ets:insert(chash_pg_table, {{vnodes, Name}, add_vnodes(NewVNodes, VNodes)}),
	    NewLinks =
		if
		    node(Pid) =:= node() ->
                link(Pid),
                [{_, LocalVNodes}] = ets:lookup(chash_pg_table, {local_vnodes, Name}),
                ets:insert(chash_pg_table, {{local_vnodes, Name}, add_vnodes(NewVNodes, LocalVNodes)}),
                [Pid | S#state.links];
		    true ->
                S#state.links
		end,
	    {reply, ok, S#state{links = NewLinks}};
	[] ->
	    {reply, no_such_group, S}
    end;

handle_call({leave, Name, Pid}, _From, S) ->
	?INFO("leave pg:~p", [{Name, Pid}]),
    case ets:lookup(chash_pg_table, {vnodes, Name}) of
        [{_, VNodes}] ->
            ets:insert(chash_pg_table, {{vnodes, Name}, delete_vnodes(Pid, VNodes)}),
            NewLinks =
                if
                    node(Pid) =:= node() ->
                        [{_, LocalVNodes}] = ets:lookup(chash_pg_table, {local_vnodes, Name}),
                        case lists:keymember(Pid, 2, LocalVNodes) of
                            true ->
                                ets:insert(chash_pg_table, {{local_vnodes, Name}, delete_vnodes(Pid, LocalVNodes)}),
                                NLinks = lists:delete(Pid, S#state.links),
                                case lists:member(Pid, NLinks) of
                                    true -> ok;
                                    false -> unlink(Pid)
                                end,
                                NLinks;
                            false ->
                                S#state.links
                        end;
                    true ->
                        S#state.links
                end,
            {reply, ok, S#state{links = NewLinks}};
        [] ->
            {reply, no_such_group, S}
    end;
 
handle_call({delete, Name}, _From, S) ->
    ets:delete(chash_pg_table, {local_vnodes, Name}),
    ets:delete(chash_pg_table, {vnodes, Name}),
    {reply, ok, S}.

handle_cast({exchange, Node, List}, S) ->
    store(List, Node),
    {noreply, S};

handle_cast({delete_vnode, Name, Pid}, S) ->
    delete_vnode(vnodes, Name, Pid),
    {noreply, S}.

handle_info({'EXIT', Pid, _}, S) ->
    Records = ets:match(chash_pg_table, {{local_vnodes, '$1'}, '$2'}),
    lists:foreach(fun([Name, VNodes]) -> 
        lists:foreach(
            fun({_Key, Pid2, _Vid} = VNode) when Pid =:= Pid2 ->
                delete_vnode(vnodes, Name, VNode),
                delete_vnode(local_vnodes, Name, VNode),
                gen_server:abcast(nodes(), ?MODULE, {delete_vnode, Name, Pid});
               (_) ->
                ok
            end, VNodes)
    end, Records),
    NewLinks = delete(S#state.links, Pid),
    {noreply, S#state{links = NewLinks}};

handle_info({nodeup, Node}, S) ->
    gen_server:cast({?MODULE, Node}, {exchange, node(), all_vnodes()}),
    {noreply, S};

handle_info({new_chash_pg, Node}, S) ->
    gen_server:cast({?MODULE, Node}, {exchange, node(), all_vnodes()}),
    {noreply, S};

handle_info({nodedown, Node}, S) ->
    Records = ets:match(chash_pg_table, {{vnodes, '$1'}, '$2'}),
    lists:foreach(fun([Name, VNodes]) -> 
        NewVNodes = lists:filter( 
            fun({_Key, Pid, _Vid}) when node(Pid) =:= Node -> false;
               (_) -> true
            end, VNodes),
        ets:insert(chash_pg_table, {{vnodes, Name}, NewVNodes})
    end, Records),
    {noreply, S};

handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    ets:delete(chash_pg_table),
    lists:foreach(fun(Pid) -> unlink(Pid) end, S#state.links).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%-----------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------
find_vnode(Hash, [{Key, _, _} = VNode | _]) 
    when Hash =< Key ->
    {ok, VNode};

find_vnode(Hash, [{Key, _, _} | T]) 
    when Hash > Key ->
    find_vnode(Hash, T);

find_vnode(_Hash, []) ->
    false.

create_vnodes(Pid, PName, VNodeNum) when is_pid(Pid) ->
    Node = node(Pid),
    lists:map(fun(I) -> 
                [_|XY] = string:tokens(pid_to_list(Pid), "."),
                PName1 =
                if PName == undefined ->
                    string:join(XY, ".");
                true ->
                    atom_to_list(PName)
                end,
                Key = chash:hash(lists:concat([
                        atom_to_list(Node), 
                        PName1, 
                        integer_to_list(I)])),
                {Key, Pid, I}
              end, lists:seq(1, VNodeNum)).

add_vnodes(NewVNodes, VNodes) ->
    lists:keysort(1, union(NewVNodes, VNodes)).

%% Delete member Pid from all groups

delete_vnode(KeyTag, Name, Pid) when is_pid(Pid) ->
    [{_, VNodes}] = ets:lookup(chash_pg_table, {KeyTag, Name}),
    ets:insert(chash_pg_table, {{KeyTag, Name}, delete_vnodes(Pid, VNodes)});

delete_vnode(KeyTag, Name, VNode) when is_tuple(VNode) ->
    [{_, VNodes}] = ets:lookup(chash_pg_table, {KeyTag, Name}),
    ets:insert(chash_pg_table, {{KeyTag, Name}, delete_vnode(VNode, VNodes)}).

delete_vnode(VNode, VNodes) ->
    lists:delete(VNode, VNodes).

delete_vnodes(Pid, VNodes) when is_pid(Pid) ->
    lists:filter(fun({_Key, Pid2, _Vid}) when Pid2 =:= Pid -> false; 
                    (_) -> true 
                 end, VNodes).


%% delete _all_ occurences of X in list
delete([X | T], X) -> delete(T, X);
delete([H | T], X) -> [H | delete(T, X)];
delete([], _) -> [].

store([[Name, VNodes] | T], Node) ->
    case ets:lookup(chash_pg_table, {vnodes, Name}) of
	[] -> 
	    ets:insert(chash_pg_table, {{vnodes, Name}, VNodes}),
	    % We can't have any local vnodes, since the group is new to us!
	    ets:insert(chash_pg_table, {{local_vnodes, Name}, []});
	[{Key, VNodes2}] ->
	    NInst = lists:keysort(1, union(VNodes, VNodes2)),
	    ets:insert(chash_pg_table, {Key, NInst})
    end,
    store(T, Node);
store([], _Node) ->
    ok.

union(L1, L2) ->
    (L1 -- L2) ++ L2.

all_vnodes() ->
    ets:match(chash_pg_table, {{vnodes, '$1'}, '$2'}).

ensure_started() ->
    case whereis(?MODULE) of
	undefined ->
	    C = {chash_pg, {?MODULE, start_link, []}, permanent,
		 1000, worker, [?MODULE]},
	    supervisor:start_child(extlib_sup, C);
	Pid ->
	    {ok, Pid}
    end.


