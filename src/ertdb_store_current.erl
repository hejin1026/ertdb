%%%----------------------------------------------------------------------
%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_current).

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/3]).

-include("elog.hrl").
-include("ertdb.hrl").

-behavior(gen_server).

-export([start_link/2,
        write/4,
		read/2
		]).
		
-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {id, rttb, his_story}).

-record(rtd, {key, time, quality=5, data, value, ref}).

start_link(HisStore, Id) ->
    gen_server:start_link({local, name(Id)}, ?MODULE, [HisStore, Id],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
name(Id) ->
    list_to_atom("ertdb_store_current_" ++ integer_to_list(Id)).		
		
write(Pid, Key, Time, Value) ->
    gen_server:cast(Pid, {write, Key, Time, Value}).
		
read(Pid, Key) ->
	gen_server:call(Pid, {read, Key}).	
		
init([HisStore, Id]) ->
    RTTB = ets:new(rttb, [set, {keypos, #rtd.key}]),
    {ok, #state{id=Id, rttb = RTTB, his_story=HisStore}}.
	
handle_call({read, Key}, _From, #state{rttb = Rttb} = State) ->
	Res = case ets:lookup(Rttb, Key) of
		[] -> 
			{ok, no_key};
		[#rtd{time=Time, quality=Quality, data=Data, value=Value}] ->
			{ok, #real_data{key=Key, time=Time, quality=Quality, data=Data, value=Value}}
	end,	
	{reply, Res, State};
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.		
	
		
handle_cast({write, Key, Time, Data}, #state{rttb=Rttb, his_story=HisStory} = State) ->
	?INFO("cur write:~p, ~p, ~p,~p", [State#state.id, Key, Time, Data]),
	case ertdb:lookup(Key) of
		[] -> 
			case ets:lookup(Rttb, Key) of
				[] -> 
					ok;
				[#rtd{time=LastTime, quality=Quality, value=LastValue}] ->	
					ertdb_store_history:write(HisStory, Key, Quality, LastTime, LastValue)
			end,	
			ets:insert(Rttb, #rtd{key=Key,time=Time,data=Data,value=Data});
			
		[#rtk_config{maxtime=Maxtime} = Config] ->
			Value = format_value(Data, Config),
			case ets:lookup(Rttb, Key) of
				[] ->
					Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
					NewRtData = #rtd{key=Key,time=Time,data=Data,value=Value,ref=Ref},
					ets:insert(Rttb, NewRtData);
				[#rtd{time=LastTime, quality=LastQuality, value=LastValue, ref=LastRef}] ->
					InsertFun = fun() ->
						?INFO("cur pass data:~p", [{new, Time, Value}]),
						cancel_timer(LastRef),
						Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
						NewRtData = #rtd{key=Key,time=Time,data=Data,value=Value,ref=Ref},
						ertdb_store_history:write(HisStory, Key, LastQuality, LastTime, LastValue, Config),
						ets:insert(Rttb, NewRtData)
					end,
								
					if(LastQuality < 5) ->
						InsertFun();
					true ->	
						case check({last,LastTime,LastValue}, {new,Time,Value}, Config) of
							true ->
								InsertFun();
							false ->
								?INFO("cur filte data:~p", [{new, Time, Value}]),
								ok
						end
				    end	
			end			
	end,
	{noreply, State};		
		
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

handle_info({maxtime, Key}, #state{rttb=Rttb, his_story=HisStory} = State) ->
	[#rtk_config{quality=Quality, maxtime=Maxtime}=Config] = ertdb:lookup(Key),
	case ets:lookup(Rttb, Key) of
		[] ->
			throw({no_key, Key});
		[#rtd{value=Value, time=LastTime, quality=Qua}=LastRtd] ->
			?INFO("cur maxtime data:~p", [LastRtd]),			
			Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
			Time = extbif:timestamp(),
			NextQua = case Quality of
					  "1" ->
						if(Qua - 1 > 1) -> Qua - 1 ;
						  true -> 1
						  end;
					 "0" ->
						Qua
					end,	 	  
			true = ets:insert(Rttb, LastRtd#rtd{time=Time, quality=NextQua, ref=Ref}),
			ertdb_store_history:write(HisStory, Key, LastTime, Qua, Value, Config)
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
	Rule = lists:concat(["> interval ", Mintime]),
	LV = extbif:to_integer(LastValue),
	Deviation = abs(extbif:to_integer(Value) - LV),
	Rule2 = lists:concat(["> deviation ", LV * Dev]),	
	judge([Rule,Rule2], [{interval, Interval}, {deviation, Deviation}]).
		
	
judge([], _Data) ->	
	true;
judge([Rule|Rs], Data) ->
	{ok, Exp} = prefix_exp:parse(Rule),
	case prefix_exp:eval(Exp, Data) of
		true -> judge(Rs, Data);
		false -> false
	end.	
	
format_value(Data, #rtk_config{coef=Coef, offset=Offset}) ->
	extbif:to_integer(Data) * Coef + Offset.
		
			
cancel_timer('undefined') -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).
			
	
	
	
	
	