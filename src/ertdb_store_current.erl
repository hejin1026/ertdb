%%%----------------------------------------------------------------------
%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_current).

-import(extbif, [zeropad/1]).

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/3]).

-include("elog.hrl").

-behavior(gen_server).

-export([start_link/0,
        % read/4, 
        write/2,
		config/2
		]).

-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {rttb}).

-record(rtd, {key, time, value, config, ref}).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
write(Pid, {Key, Time, Value}) ->
    gen_server2:cast(Pid, {write, Key, Time, Value}).
		
config(Pid, {Key, Config}) ->
	gen_server2:cast(Pid, {config, Key, Config}).		
		
init([]) ->
	RTTB = ets:new(rttb, [set, {keypos, #rtd.key}]),
	{ok, #state{rttb = RTTB}}.
	
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.		
	
handle_cast({config, Key, Config}, #state{rttb = Rttb} = State) ->
	case ets:lookup(Rttb, Key) of
		[] ->
			ets:insert(Rttb, #rtd{key=Key,config=Config});
		[RtData] ->
			ets:insert(Rttb, RtData#rtd{config=Config})
	end,
	{noreply, State};				
		
handle_cast({write, Key, Time, Value}, #state{rttb = Rttb} = State) ->
	case ets:lookup(Rttb, Key) of
		[] -> %unconfig
			ets:insert(Rttb, #rtd{key=Key,time=Time,value=Value});
		[#rtd{time=Time, value=Value,config=Config, ref=LastRef}] ->
			case check({last,Time,Value}, {new,Time,Value},Config) of
				true ->
					cancel_timer(LastRef),
					Maxtime = propslist:get_value(cur_maxtime, Config),
					Ref = erlang:send_after(Maxtime, self(), {maxtime, Key}),
					NewRtData = #rtd{key=Key,time=Time,value=Value,config=Config,ref=Ref},
					ets:insert(Rttb, NewRtData),
					ertdb_store_history:write(Key, Time, Value, Config);
				false ->
					ok
			end			
	end,
	{noreply, State};		
		
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.
	
handle_info({maxtime, Key}, #state{rttb = Rttb} = State) ->
	case ets:lookup(Rttb, Key) of
		[] ->
			throw({no_key, Key});
		[#rtd{value=Value, config=Config}=LastRtd] ->			
			Maxtime = propslist:get_value(cur_maxtime, Config),
			Ref = erlang:send_after(Maxtime, self(), {maxtime, Key}),
			Time = ertdb_util:timestamp(),
			ets:insert(Rttb, LastRtd#rtd{time=Time, ref=Ref}),
			ertdb_store_history:write(Key, Time, Value, Config)
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
check({last, Lastime, LastValue}, {new, Time, Value}, Config) ->
	Rule0 = "= has_cur 1",
	Interval = Time - Lastime,
	Rule = "&(> interval cur_mintime)(< interval cur_maxtime)",
	Deviation = abs(Value - LastValue),
	Rule2 = "> deviation cur_deviation",	
	judge([Rule0,Rule,Rule2], [{interval, Interval}, {deviation, Deviation}|Config]).
		
	
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
			