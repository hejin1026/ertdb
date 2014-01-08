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
	handle(Method, Path, Req).

handle('GET', {"rrdb", Key, "last"}, Req) ->
    folsom_metrics:notify({'http.last', {inc, 1}}),
	case errdb:last(unquote(Key)) of
    {ok, Time, Fields, Values} ->
        Resp = ["TIME:", join(Fields, ","), "\n", errdb_lib:line(Time, Values)],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rrdb", RawKey, "last", RawFields}, Req) ->
    folsom_metrics:notify({'http.last', {inc, 1}}),
	Key = unquote(RawKey),
	Fields = unquote(RawFields),
	case errdb:last(Key, tokens(Fields, ",")) of
    {ok, Time, Values} -> 
        Resp = ["TIME:", Fields, "\n", errdb_lib:line(Time, Values)],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle('GET', {"rrdb", RawKey, RawFields, RawRange}, Req) ->
    folsom_metrics:notify({'http.fetch', {inc, 1}}),
	Key = unquote(RawKey),
	Fields = unquote(RawFields),
	Range = unquote(RawRange),
    [Begin, End] = tokens(Range, "-"),
	case errdb:fetch(Key, tokens(Fields, ","),
        list_to_integer(Begin), list_to_integer(End)) of
    {ok, Records} -> 
        Lines = join([errdb_lib:line(Time, Values) || {Time, Values} <- Records], "\n"),
        Resp = ["TIME:", Fields, "\n", Lines],
        Req:ok({"text/plain", Resp});
    {error, Reason} ->
		?WARNING("~s ~p", [Req:get(raw_path), Reason]),
        Req:respond({500, [], atom_to_list(Reason)})
	end;

handle(_Other, _Path, Req) ->
	Req:respond({404, [], <<"bad request, path not found.">>}). 