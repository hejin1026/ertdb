%%%----------------------------------------------------------------------
%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_current).

-import(extbif, [zeropad/1]).

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/3]).

-include("elog.hrl").
-include("ertdb.hrl").

-behavior(gen_server).

-export([start_link/0,
        write/4,
		read/2
		]).
		
-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {rttb}).

-record(rtd, {key, time, value, ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
write(Pid, Key, Time, Value) ->
    gen_server:cast(Pid, {write, Key, Time, Value}).
		
read(Pid, Key) ->
	gen_server:call(Pid, {read, Key}).	
		
init([]) ->
    RTTB = ets:new(rttb, [set, {keypos, #rtd.key}]),
    {ok, #state{rttb = RTTB}}.
	
handle_call({read, Key}, _From, #state{rttb = Rttb} = State) ->
	Res = case ets:lookup(Rttb, Key) of
		[] -> {ok, no_key};
		[#rtd{time=Time, value=Value}] ->
			{ok, {Time, Value}}
	end,	
	{reply, Res, State};
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.		
	
		
handle_cast({write, Key, Time, Value}, #state{rttb = Rttb} = State) ->
	?INFO("cur write:~p, ~p,~p", [Key, Time, Value]),
	case ertdb:lookup(Key) of
		[] -> 
			ets:insert(Rttb, #rtd{key=Key,time=Time,value=Value});
		[#rtk_config{maxtime=Maxtime} = Config] ->
			case ets:lookup(Rttb, Key) of
				[] ->
					Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
					NewRtData = #rtd{key=Key,time=Time,value=Value,ref=Ref},
					ets:insert(Rttb, NewRtData);
				[#rtd{time=LastTime, value=LastValue,ref=LastRef}] ->
					case check({last,LastTime,LastValue}, {new,Time,Value}, Config) of
						true ->
							?INFO("cur pass data:~p", [{new, Time, Value}]),
							cancel_timer(LastRef),
							Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
							NewRtData = #rtd{key=Key,time=Time,value=Value,ref=Ref},
							ertdb_store_history:write(ertdb_store_history, Key, LastTime, LastValue, Config),
							ets:insert(Rttb, NewRtData);
						false ->
							?INFO("cur filte data:~p", [{new, Time, Value}]),
							ok
					end
			end			
	end,
	{noreply, State};		
		
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.
	
handle_info({maxtime, Key}, #state{rttb = Rttb} = State) ->
	[#rtk_config{maxtime=Maxtime}=Config] = ertdb:lookup(Key),
	case ets:lookup(Rttb, Key) of
		[] ->
			throw({no_key, Key});
		[#rtd{value=Value, time=LastTime}=LastRtd] ->
			?INFO("cur maxtime data:~p", [LastRtd]),			
			Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
			Time = extbif:timestamp(),
			true = ets:insert(Rttb, LastRtd#rtd{time=Time, ref=Ref}),
			ertdb_store_history:write(ertdb_store_history, Key, LastTime, Value, Config)
	end,	
	{noreply, State};
		
	
handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.	

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
	
%%-----inter fun----%%	
check({last, Lastime, LastValue}, {new, Time, Value}, #rtk_config{dev=Dev, mintime=Mintime, maxtime=Maxtime}) ->
	Interval = Time - Lastime,
	Rule = lists:concat(["&(> interval ", Mintime,")(< interval ", Maxtime, ")"]),
	Deviation = abs(extbif:to_integer(Value) - extbif:to_integer(LastValue)),
	Rule2 = lists:concat(["> deviation ", Dev]),	
	judge([Rule,Rule2], [{interval, Interval}, {deviation, Deviation}]).
		
	
judge([], _Data) ->	
	true;
judge([Rule|Rs], Data) ->
	{ok, Exp} = prefix_exp:parse(Rule),
	case prefix_exp:eval(Exp, Data) of
		true -> judge(Rs, Data);
		false -> false
	end.	
			
cancel_timer('undefined') -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).
			