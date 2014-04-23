-module(ertdb_client_test).

-include("ertdb.hrl").

-compile([export_all]).


run() ->
	Key = build_key(1, 11, 2),
	C = c(),
	% config(C, Key, [{compress, 1}, {dev, 8}, {his_dev, 16}, {maxtime, 60}, {mintime, 10}, {his_maxtime, 300}]),
	timer:sleep(20),
	io:format("rtk config:~p", [ertdb:lookup(Key)]),
	lists:foreach(fun(_Item) ->
		insert(C, Key, extbif:timestamp(), random:uniform(200)),
		timer:sleep(2000)
	end, lists:seq(1, 6)).
	% fetch_test(C, Key).
	
	
%% command
	% Key = ertdb_client_test:build_key(1, 11, 2).
	% C = ertdb_client_test:c().
	
	% 配置 及 查询
	% ertdb_client_test:config(C, Key, [{coef,0.1},{offset,-10},{compress, 1}, {dev, 0.2}, {his_dev, 0.1}, {maxtime, 60}, {mintime, 0}, {his_maxtime, 300}]).
	% ertdb:lookup(Key).
	
	% 插入值 及 实时值查询
	% ertdb_client_test:insert(C, Key, extbif:timestamp(), 123).
	% ertdb_client_test:fetch(C, Key).	
	% ertdb:fetch(Key).
			
	% 历史值 及 历史值内存查询		
	% ertdb_client_test:fetch(C, Key, extbif:timestamp({{2014,1,6}, time()}), extbif:timestamp()).	
	% ertdb:fetch(Key, extbif:timestamp({{2014,4,23}, time()}), extbif:timestamp()).
	% ertdb:lookup_his(Key).
	
	
	% {ok, [{Name, DataFd, Indices}]} = gen_server:call(ertdb_store_history, {read_idx, Key, extbif:timestamp({{2014,1,6}, time()}), extbif:timestamp()}).
	% file:pread(DataFd, Indices).
	
	
	% ets:lookup(chash_pg_table, {vnodes, ertdb}).
	% ets:lookup(chash_pg_table, {local_vnodes, ertdb}).
	% chash_pg:get_pid(ertdb, Key).
			

config(C, Key, Data) ->
	Config = [lists:concat([K, "=", V]) || {K, V} <- Data],
	Cmd = ["config", Key, string:join(Config, ",")],
	Res = ertdb_client:q(C, Cmd),
	io:format("config res:~p ~n", [Res]).
	
	
insert(C, Key, Time, Value) ->
	Cmd = ["insert", Key, Time, Value],
	Res = ertdb_client:q_noreply(C, Cmd),
	io:format("insert res:~p ~n", [Res]).
	
fetch(C, Key) ->
	Cmd = ["fetch", Key],
	Res = ertdb_client:q(C, Cmd),
	io:format("fetch res:~p ~n", [Res]).
	
fetch(C, Key, Begin, End) ->
	Cmd = ["fetch", Key, Begin, End],
	Res = ertdb_client:q(C, Cmd),
	io:format("fetch res:~p ~n", [Res]).		


build_key(Cid, Type, No) ->
	list_to_binary(lists:concat([Cid, ":", Type, ":", No])).
	

c() ->
    {ok, C} = ertdb_client:start_link(),
    C.	