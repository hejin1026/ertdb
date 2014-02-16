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


handle('GET', {"rtdb.json", Key}, Req) ->
	% ?INFO("get key:~p, ~p", [Key, unquote(Key)]),
	case ertdb:fetch(list_to_binary(unquote(Key))) of
    {ok, {Time, Value}} ->
        Resp = [{time, Time}, {value, Value}],
        Req:ok({"text/plain", jsonify(Resp)});
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rtdb", Key}, Req) ->
	% ?INFO("get key:~p, ~p", [Key, unquote(Key)]),
	case ertdb:fetch(list_to_binary(unquote(Key))) of
    {ok, {Time, Value}} ->
        Resp = ["TIME:Value\n",format_data({Time, Value})],
        Req:ok({"text/plain", Resp});
	{ok, Other} ->
		Req:ok({"text/plain", extbif:to_list(Other)});	
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rtdb", Key, "lookup"}, Req) ->
	% ?INFO("get key:~p, ~p", [Key, unquote(Key)]),
	case  ertdb_store_history:lookup(ertdb_store_history, list_to_binary(unquote(Key)))of
    {ok, Records} -> 
        Lines = join([format_data(Item) || Item <- Records], "\n"),
        Resp = ["TIME:Value\n", Lines],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rtdb", RawKey, RawRange}, Req) ->
	Key = unquote(RawKey),
	Range = unquote(RawRange),
    {BeginT, EndT} = case tokens(Range, "-") of
		[Begin] ->
			{extbif:timestamp(to_datetime(Begin)), extbif:timestamp()};	
		[Begin, End] ->
			{extbif:timestamp(to_datetime(Begin)), extbif:timestamp(to_datetime(End))}
	end,		
	?INFO("begin:~p, end:~p", [extbif:datetime(BeginT), extbif:datetime(EndT)]),
	case ertdb:fetch(list_to_binary(Key), BeginT, EndT) of
    {ok, Records} -> 
        Lines = join([format_data(Item) || Item <- Records], "\n"),
        Resp = ["TIME:Value\n", Lines],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;
	

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
	
format_data({Time, Value}) ->
	lists:concat([extbif:strftime(extbif:datetime(Time)), " ==> ", binary_to_list(Value)]);
format_data(Data) ->
	Data.		
	

to_string(T)  ->
    lists:flatten(io_lib:format("~p", [T])).		
	
jsonify(Term) ->
    Encoder = mochijson2:encoder([]),
    list_to_binary(Encoder(Term)).	
	
	
	
	
	
	
		
	