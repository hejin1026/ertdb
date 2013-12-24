%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb_socket).

-export([start/1, 
        loop/1, 
        stop/0]).

-define(TIMEOUT, 3600000).

-include("elog.hrl").

%% External API
start(Options) ->
    ?INFO_MSG("errdb_socket is started."),
    mochiweb_socket_server:start([{name, ?MODULE}, 
        {loop, fun loop/1} | Options]).

stop() ->
    mochiweb_socket_server:stop(?MODULE).

trim(Line) ->
	[H|_] = binary:split(Line, [<<"\r\n">>], [global, trim]), H.

loop(Socket) ->
    inet:setopts(Socket, [{packet, line},{keepalive, true}]),
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
    {ok, Line} -> 
        case handle_req(binary_to_list(trim(Line))) of
		{reply, Reply} ->
			case gen_tcp:send(Socket, Reply) of
			ok -> loop(Socket);
			_ -> exit(normal)
			end;
		noreply ->
			loop(Socket);
		{stop, _Err} ->
			exit(normal)
        end;
    {error, closed} ->
        gen_tcp:close(Socket),
        exit(normal);
    {error, timeout} ->
        gen_tcp:close(Socket),
        exit(normal);
    Other ->
        ?ERROR("bad socket data: ~p", [Other]),
        gen_tcp:close(Socket),
        exit(normal)
    end.

handle_req(Line) when is_list(Line) ->
	handle_req(list_to_tuple(string:tokens(Line, " ")));
	
handle_req({"config", Object, Config}) ->
	try ertdb:config(Object, Config) 
	catch _:Error -> ?ERROR("error config:~p, ~p, ~p", [Error, Object, Config])
	end,
	noreply;	

handle_req({"insert", Object, Time, Value}) ->
	try ertdb:insert(Object, list_to_integer(Time), Value) 
	catch _:Error -> ?ERROR("error insert:~p, ~p", [Error, Value])
	end,
	noreply;
	
handle_req({"fetch", Object, Begin, End}) ->
	try ertdb:fetch(Object, Begin, End) 
	catch _:Error -> ?ERROR("error fetch:~p, ~p", [Error, Value])
	end,
	noreply;	

handle_req(Req) ->
    ?ERROR("badreq: ~p", [Req]),
	{stop, badreq}.
