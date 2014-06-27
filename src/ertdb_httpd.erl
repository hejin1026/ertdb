%%%----------------------------------------------------------------------
%%% Author  : hejin1026@gmail.com
%%% Purpose : http send packet
%%% Created : 2013-12-2
%%%----------------------------------------------------------------------
-module(ertdb_httpd).

-include("elog.hrl").

-import(string, [join/2, tokens/2]).

-import(mochiweb_util, [unquote/1]).

-export([start/1,
        loop/1,
        stop/0]).
		
-include("ertdb.hrl").		

%% External API
start(Options) ->
    ?INFO_MSG("ertdb_httpd is started."),
    mochiweb_http:start([{name, ?MODULE}, {loop, fun loop/1} | Options]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req) ->
    Method = Req:get(method),
	?INFO("~s ~s", [Method, Req:get(raw_path)]),
	Path = list_to_tuple(string:tokens(Req:get(raw_path), "/")),
	try
		handle(Method, Path, Req)
	catch Type:Error ->
		?ERROR("error :~p, ~p, ~p", [Type, Error, erlang:get_stacktrace()]),
		Req:respond({404, [], <<"catch exception">>})
	end.		

% 实时值
handle('GET', {"rtdb", Key}, Req) ->
	% ?INFO("get key:~p, ~p", [Key, unquote(Key)]),
	case ertdb:fetch(list_to_binary(unquote(Key))) of
     {ok, #real_data{time=Time, quality=Quality, data=Data, value=Value}} ->
		Resp = [{time, Time}, {quality, Quality}, {data, Data}, {value, extbif:to_binary(Value)}],
        Req:ok({"text/plain", jsonify(Resp)});
	{ok, no_key} ->
		Req:ok({"text/plain", [{value, <<"no_key">>}] });	
	{ok, invalid_key} ->
		Req:ok({"text/plain", [{value, <<"invalid_key">>}] });		
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;


handle('POST', {"rtdb", "multiple.json"}, Req) ->
    Params = Req:parse_post(), 
    ?INFO("get params :~p", [Params]),
	Keys = proplists:get_value("keys", Params),
	Records = lists:foldl(fun(Key, Acc) ->
		case ertdb:fetch(list_to_binary(unquote(Key))) of
		    {ok, #real_data{time=Time, quality=Quality, data=Data, value=Value}} ->
				?INFO("key:~p", [Key]),
				[ [{key, list_to_binary(Key)}, {time, Time}, {data, Data}, {value, extbif:to_binary(Value)}, {quality, Quality}] | Acc];
			{ok, no_key} ->
				[ [{key, list_to_binary(Key)}, {quality, 0}, {value, 0}] | Acc];
			{ok, invalid_key} ->
				Acc;
		    {error, Reason} ->
				?ERROR("multipe error:~p,~p", [Key,Reason]),
		        [ [{key, list_to_binary(Key)}, {error, Reason}] | Acc]
		end
	end, [], string:tokens(Keys, ",")),
    Req:ok({"text/plain", jsonify(Records)});
	

% 历史值
handle('GET', {"rtdb", RawKey, RawRange}, Req) ->
	% ?INFO("get key:~p, ~p", [Key, unquote(Key)]),
	Key = unquote(RawKey),
	Range = unquote(RawRange),
    {BeginT, EndT} = case tokens(Range, "-") of
		[Begin] ->
			{list_to_integer(Begin), extbif:timestamp()};	
		[Begin, End] ->
			{list_to_integer(Begin), list_to_integer(End)}
	end,		
	?INFO("begin:~p, end:~p", [extbif:datetime(BeginT), extbif:datetime(EndT)]),
	case ertdb:fetch(list_to_binary(Key), BeginT, EndT) of
	    {ok, Records} -> 
	        Resp = [[{time, Time}, {quality, Quality}, {value, extbif:to_binary(Value)}] || {Time, Quality, Value} <- Records],
	        Req:ok({"text/plain",  jsonify(Resp)});
	    {error, Reason} ->
			?WARNING("~s ~p", [Req:get(raw_path), Reason]),
	        Req:respond({500, [], atom_to_list(Reason)})
	end;
	
	
handle('POST', {"rtdb", "his_multiple.json"}, Req) ->
    Params = Req:parse_post(), 
    ?INFO("get params :~p", [Params]),
	Keys = proplists:get_value("keys", Params),
	BeginTime = list_to_integer(proplists:get_value("begintime", Params)),
	EndTime = list_to_integer(proplists:get_value("endtime", Params)),
	DataLists = lists:foldl(fun(Key, Acc) ->
		case ertdb:fetch(list_to_binary(unquote(Key)), BeginTime, EndTime) of
		    {ok, Records} -> 
				Data = [[{time, Time}, {quality, Quality}, {value, extbif:to_binary(Value)}] || {Time, Quality, Value} <- Records],
				[ [{key, list_to_binary(Key)}, {data, Data}] | Acc];
		    {error, Reason} ->
				?WARNING("~s ~p", [Req:get(raw_path), Reason]),
				Acc
		end
	end, [], string:tokens(Keys, ",")),
    Req:ok({"text/plain", jsonify(DataLists)});
		

handle(_Other, _Path, Req) ->
	Req:respond({404, [], <<"bad request, path not found.">>}). 
	
to_datetime(StrfDatetime) when is_list(StrfDatetime), length(StrfDatetime) >= 8->
	case length(StrfDatetime) of
		8 ->
			Y = string:sub_string(StrfDatetime, 1, 4),
			M = string:sub_string(StrfDatetime, 5, 6),
			D = string:sub_string(StrfDatetime, 7, 8),
			{{list_to_integer(Y), list_to_integer(M), list_to_integer(D)}, {0,0,0}};
		14 ->
			Y = string:sub_string(StrfDatetime, 1, 4),
			M = string:sub_string(StrfDatetime, 5, 6),
			D = string:sub_string(StrfDatetime, 7, 8),
			H = string:sub_string(StrfDatetime, 9, 10),
			MM = string:sub_string(StrfDatetime, 11, 12),
			S =	string:sub_string(StrfDatetime, 13, 14), 
			{{list_to_integer(Y), list_to_integer(M), list_to_integer(D)}, 
				{list_to_integer(H), list_to_integer(MM), list_to_integer(S)}};
		_ ->
			to_datetime(string:left(StrfDatetime, 14, $0))
	end.			
	
format_data({Time, Quality, Value} ) ->
	lists:concat([extbif:strftime(extbif:datetime(Time)), " ==> ", Quality, "#", Value]);	
format_data(Data) ->
	Data.		
	

to_string(T)  ->
    lists:flatten(io_lib:format("~p", [T])).
	
jsonify(Term) ->
    Encoder = mochijson2:encoder([]),
    Encoder(Term).	
	
	
	
	
	
	
		
	