-module(ertdb_esockd).

-export([start/1, stop/1]).
	
-export([start_link/1, 
		 init/1, 
		 loop/2]).
		 
-include("elog.hrl").
-include("ertdb.hrl").		 

-define(TCP_OPTIONS, [binary, {packet, raw}, {reuseaddr, true}, {backlog, 1024}, {nodelay, false}, {recbuf, 16 * 1024}]). %{backlog, }, 

start(SocketConf) ->
	Port = proplists:get_value(port, SocketConf),
    esockd:listen(ertdb, Port, ?TCP_OPTIONS, {ertdb_esockd, start_link, []}).

stop(Port) ->
    esockd:stop(ertdb, Port).
	

start_link(Sock) ->
	Pid = spawn_link(?MODULE, init, [Sock]),
	{ok, Pid}.

init(Sock) ->
	esockd_client:accepted(Sock),
	?INFO("get connnect from client...",[]),
	loop(Sock, ertdb_parser:init()).

loop(Sock, ParserState) ->
	case gen_tcp:recv(Sock, 0) of
		{ok, Data} -> 
			mainloop(Sock, ParserState, Data);
		{error, Reason} ->
			io:format("tcp ~s~n", [Reason]),
			{stop, Reason}
	end. 	
		
mainloop(Socket, ParserState, Data) ->
    case ertdb_parser:parse(ParserState, Data) of
        %% Got complete response, return value to client
        {ReturnCode, Value, NewParserState} ->
            handle_req(Socket, {ReturnCode, Value}),
			loop(Socket, NewParserState);
			
        %% Got complete response, with extra data, reply to client and
        %% recurse over the extra data
        {ReturnCode, Value, Rest, NewParserState} ->
            handle_req(Socket, {ReturnCode, Value}),
            mainloop(Socket, NewParserState, Rest);

        %% Parser needs more data, the parser state now contains the
        %% continuation data and we will try calling parse again when
        %% we have more data
        {continue, NewParserState} ->
            loop(Socket, NewParserState)
	end.		

close(Socket) ->
    ?ERROR("socket close:~p",[self()]),
    gen_tcp:close(Socket),
    exit(normal).			 

	
handle_req(Socket, Data) ->	
	case handle_reply(Data) of
		{reply, Reply} ->
			try
				?INFO("reply:~p", [Reply]),
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
	lists:concat(["$", length(Data), ?NL2, Data, ?NL2]);
build_response({error, Reason}) ->		
	lists:concat(["-", "error", " ", Reason, ?NL2]);	
build_response(Rest) when is_integer(Rest) ->
	lists:concat([":", Rest, ?NL2]);	
build_response(Rest) when is_atom(Rest) ->
	lists:concat(["+", Rest, ?NL2]);
build_response(Rest) when is_list(Rest) ->
	Command = [build_response({ok, R})||R <- Rest],
	?INFO("get length:~p", [length(Rest)]),
	lists:concat(["*", length(Rest), ?NL2, string:join(Command, "")]);
build_response(Rest) ->
	throw({unsupport_build, Rest}).		

format_data(#real_data{time=Time, quality=Quality, value=Value}) -> % real
	lists:concat([Time, ":", Quality, ":", extbif:to_list(Value)]);
format_data({Time, Quality, Value}) ->	% history
	lists:concat([Time, ":", Quality, ":", extbif:to_list(Value)]);
format_data(Data) ->
	Data.		