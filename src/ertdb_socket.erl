%%%----------------------------------------------------------------------
%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb_socket).

-export([start/1, 
        loop/1, 
        stop/0]).

-define(RECV_TIMEOUT, 3600 * 24 * 10).

-include("elog.hrl").
-include("ertdb.hrl").


%% External API
start(Options) ->
    ?INFO_MSG("ertdb_socket is started."),
    mochiweb_socket_server:start([{name, ?MODULE}, 
        {loop, fun loop/1} | Options]).

stop() ->
    mochiweb_socket_server:stop(?MODULE).


loop(Socket) ->
    inet:setopts(Socket, [{keepalive, true}]),
    recvloop(Socket, ertdb_parser:init()).
	
recvloop(Socket, ParserState) ->
	inet:setopts(Socket, [{active, once}]),
	receive
		{tcp, Socket, Data} ->
		    mainloop(Socket, ParserState, Data);
	    {tcp_closed, Socket} ->
	        close(Socket);
		{tcp_error, Socket, Reason} -> 
			?ERROR("tcp error:~p",[Reason]),
			close(Socket);
	    Other ->
	        ?ERROR("invalid request: ~p", [Other]),
	        loop(Socket)
	    after ?RECV_TIMEOUT ->
	        ?ERROR("tcp timeout...:~p", [Socket]),
	        close(Socket)
	    end.	
		
mainloop(Socket, ParserState, Data) ->
    try ertdb_parser:parse(ParserState, Data) of
        %% Got complete response, return value to client
        {ReturnCode, Value, NewParserState} ->
            handle_req(Socket, {ReturnCode, Value}),
			recvloop(Socket, NewParserState);
			
        %% Got complete response, with extra data, reply to client and
        %% recurse over the extra data
        {ReturnCode, Value, Rest, NewParserState} ->
            handle_req(Socket, {ReturnCode, Value}),
            mainloop(Socket, NewParserState, Rest);

        %% Parser needs more data, the parser state now contains the
        %% continuation data and we will try calling parse again when
        %% we have more data
        {continue, NewParserState} ->
            recvloop(Socket, NewParserState)
	catch 
		exit:Reason ->
	        ?ERROR("packet error...:~p", [Reason]),
			gen_tcp:send(Socket, build_response({error, Reason})),
			timer:sleep(100),
	        close(Socket)
    end.		

close(Socket) ->
    ?ERROR("socket close:~p",[self()]),
    gen_tcp:close(Socket),
    exit(normal).			 

	
handle_req(Socket, Data) ->	
	case handle_reply(Data) of
		{reply, Reply} ->
			try
				Packet = build_response(Reply),
				case gen_tcp:send(Socket, Packet) of
					ok -> ok;
					_ -> exit(normal)
				end
		    catch
		        Type:Error -> 
					?ERROR("error reply:~p, ~p, ~p", [Type, Error, erlang:get_stacktrace()])
		    end;
		noreply ->
			ok;
		{stop, _Err} ->
			close(Socket)
    end.
    

handle_reply({ok, Data}) when is_list(Data) ->
	handle_reply(list_to_tuple(Data));
	
handle_reply({<<"config">>, Key, Config}) ->
	Rest =  
		try 
			ertdb:config(Key, Config)
		catch Type:Error ->
			?ERROR("error config:~p, ~p, ~p", [Type, Error, erlang:get_stacktrace()]),
			{error, "config fail"}	
		end,		
	{reply, Rest};	
			

handle_reply({<<"insert">>, Key, Time, Value}) ->
	ertdb:insert(Key, list_to_integer(binary_to_list(Time)), Value), 
	noreply;

handle_reply({<<"fetch">>, Key}) ->	
	Rest = ertdb:fetch(Key), 
	?INFO("fetch rest:~p", [Rest]),
	{reply, Rest};
	
handle_reply({<<"fetch">>, Key, Begin, End}) ->
	{ok, Datalist} = ertdb:fetch(Key, Begin, End),
	{reply, Datalist};	

handle_reply(Req) ->
    ?ERROR("badreq: ~p", [Req]),
	{stop, badreq}.

	
build_response({ok, Rest}) ->
	Data = format_data(Rest),
	lists:concat(["$", length(Data), ?NL, Data, ?NL]);
build_response({error, Reason}) ->		
	lists:concat(["-", "error", " ", Reason, ?NL]);	
build_response(Rest) when is_integer(Rest) ->
	lists:concat([":", Rest, ?NL]);	
build_response(Rest) when is_atom(Rest) ->
	lists:concat(["+", Rest, ?NL]);
build_response(Rest) when is_list(Rest) ->
	Command = [build_response({ok, R})||R <- Rest],
	?INFO("get length:~p", [length(Rest)]),
	lists:concat(["*", length(Rest), ?NL, string:join(Command, "")]);
build_response(Rest) ->
	throw({unsupport_build, Rest}).		

format_data({Time, Value}) ->
	lists:concat([Time, ":", binary_to_list(Value)]);
format_data(Data) ->
	Data.	
	
	
