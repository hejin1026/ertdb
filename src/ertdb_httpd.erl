%%%----------------------------------------------------------------------
%%% Author  : hejin1026@gmail.com
%%% Purpose : http send packet
%%% Created : 2013-12-2
%%%----------------------------------------------------------------------
-module(ertdb_httpd).

-author('hejin').

-include("elog.hrl").

-export([start/1]).

-export([init/3,
		handle/2,
		terminate/3]).
		
-define(MAX_SIZE, 1024*1024).		

%% External API
start(Opts) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/measure", ertdb_httpd, []}
		]}
	]),
	%% Name, NbAcceptors, TransOpts, ProtoOpts
	{ok, Pid} = cowboy:start_http(?MODULE, 100, Opts,
		[{env, [{dispatch, Dispatch}]}]
	),
	Port = proplists:get_value(port, Opts),
	?PRINT("Extend104 Httpd is listening on ~p~n", [Port]),
	{ok, Pid}.
	

init(_Transport, Req, []) ->
	{ok, Req, undefined}.

handle(Req, State) ->
    case cowboy_req:body_length(Req) of
    {Length, Req1} when is_integer(Length) and (Length > ?MAX_SIZE) -> 
		{ok, reply400(Req1, "body is too big."), State}; 
	{_, Req1} ->
		{Method, Req2} = cowboy_req:method(Req1),
		{Path, Req3} = cowboy_req:path(Req2),
		QsFun = qsfun(Method),
		{ok, Vals, Req4} = QsFun(Req3),
		Params = [{b2a(K), V} || {K, V} <- Vals],
		handle(Req4, Method, Path, Params, State)
	end.
	
qsfun(<<"GET">>) -> 
	fun(Req) -> {Vals, Req1} = cowboy_req:qs_vals(Req), {ok, Vals, Req1} end;

qsfun(<<"POST">>) ->
	fun(Req) -> cowboy_req:body_qs(?MAX_SIZE, Req) end.	

handle(Req, <<"GET">>, <<"/measure">>, Params, State) ->
    Ip = proplists:get_value(ip, Params),
	Port = proplists:get_value(port, Params),    
	ConnOid = #extend104_oid{ip=Ip, port=binary_to_integer(Port)},
	case extend104:get_conn_pid(ConnOid) of
	{ok, ConnPid} -> 
		MeaType = proplists:get_value(measure_type, Params, '$_'),
		MeaNo = proplists:get_value(measure_no, Params, '_'),
		{ok, Meas} = extend104_connection:get_measure(ConnPid, {MeaType, MeaNo}),
		?INFO("get meas:~p", [Meas]),
		{ok, Reply} = cowboy_req:reply(200, [{"Content-Type", "text/plain"}], format(Meas), Req), 
		{ok, Reply, State};
	error ->
		{ok, Reply} = cowboy_req:reply(400, [{"Content-Type", "text/plain"}], <<"can not find conn">>, Req), 
		{ok, Reply, State}
	end;	

handle(Req, Method, Path, Params, State) ->
	{Peer, Req1} = cowboy_req:peer(Req),
    ?ERROR("bad request from ~p: ~p ~p: ~p", [Peer, Method, Path, Params]),
	{ok, Reply} = cowboy_req:reply(400, [{"Content-Type", "text/plain"}], <<"bad request">>, Req1), 
	{ok, Reply, State}.

terminate(_Reason, _Req, _State) ->
	ok.	
	
reply400(Req, Msg) ->
	{ok, Reply} = cowboy_req:reply(400, [], Msg, Req), 
	Reply.	
	
format(Meas) ->
	string:join(lists:map(fun(#measure{id=Id, value=V}) ->
		lists:concat([Id#measure_id.type,'-', Id#measure_id.no,':'])  ++ extend104_util:to_string(V)
	end, Meas),"\n").
			