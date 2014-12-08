%%%----------------------------------------------------------------------
%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_current).

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/3]).

-include("elog.hrl").
-include("ertdb.hrl").

-behavior(gen_server2).

-export([start_link/3,
        write/4,
		read/2,
		lookup_info/1,
		name/1
		]).
		
-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {id, rttb, his_story, rtk_config}).

-record(rtd, {key, time, quality=5, data, value, ref}).

start_link(Id, HisStore, RtkConfig) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id, HisStore, RtkConfig],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
name(Id) ->
	ertdb_util:name("ertdb_store_current", Id).
			
write(Pid, Key, Time, Value) ->
    gen_server2:cast(Pid, {write, Key, Time, Value}).
		
read(Pid, Key) ->
	gen_server2:call(Pid, {read, Key}).	
	
lookup_info(Pid) ->
	gen_server2:call(Pid, ets_info).	
	
		
init([Id, HisStore, RtkConfig]) ->
    RTTB = ets:new(ertdb_util:name("ertdb_rttb", Id), [set, {keypos, #rtd.key}, named_table]),
    {ok, #state{id=Id, rttb = RTTB, his_story=HisStore, rtk_config=RtkConfig}}.
	
	
handle_call(ets_info, _From, #state{rttb = Rttb} = State) ->
	Res = ets:info(Rttb, size), 
	{reply, Res, State};
		
	
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
	
		
handle_cast({write, Key, Time, Data}, #state{rttb=Rttb, his_story=HisStory, rtk_config=RtkConfig} = State) ->
	?INFO("cur write:~p, ~p, ~p,~p", [State#state.id, Key, Time, Data]),
	case ets:lookup(RtkConfig, Key)	of
		[] -> 
			ets:insert(Rttb, #rtd{key=Key,time=Time,data=Data,value=Data}),
			ertdb_store_history:write(HisStory, Key, Time, 5, Data);
			
		[#rtk_config{maxtime=Maxtime} = Config] ->
			Value = format_value(Data, Config),
			case ets:lookup(Rttb, Key) of
				[] ->
					Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
					NewRtData = #rtd{key=Key,time=Time,data=Data,value=Value,ref=Ref},
					ets:insert(Rttb, NewRtData),
					ertdb_store_history:write(HisStory, Key, Time, 5, Value, Config);
				[#rtd{time=LastTime, quality=LastQuality, value=LastValue, ref=LastRef}] ->
					InsertFun = fun() ->
						?INFO("cur pass data:~p", [{new, Time, Value}]),
						case ertdb_util:cancel_timer(LastRef) of
							false ->
								?ERROR("cancel fail,timeout has send:~p, lastime:~p, maxtime:~p, value:~p", 
										[Key, LastTime, Maxtime, Value]);
							_ ->
								ok
						end,
						Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
						NewRtData = #rtd{key=Key,time=Time,data=Data,value=Value,ref=Ref},
						ets:insert(Rttb, NewRtData),
						ertdb_store_history:write(HisStory, Key, Time, 5, Value, Config)
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

handle_info({maxtime, Key}, #state{rttb=Rttb, his_story=HisStory, rtk_config=RtkConfig} = State) ->
	case ets:lookup(RtkConfig, Key) of
		[] ->
			% 可能ertdb死掉配置清掉了
			?ERROR("no ertdb config:~p", [Key]);
		[#rtk_config{quality=IsQuality, maxtime=Maxtime}=Config] ->	
			[#rtd{value=Value, time=LastTime, quality=LastQua}=LastRtd] = ets:lookup(Rttb, Key),
				?INFO("cur maxtime data:~p", [LastRtd]),			
				% Time = extbif:timestamp(), 机器时间可能不一致
				Time = LastTime + Maxtime,
				Quality  = case check_quality(IsQuality) of
					  true ->
						  if(LastQua - 1 > 1) -> 
							  Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
							  ets:insert(Rttb, LastRtd#rtd{time=Time, quality= LastQua - 1, ref=Ref});
						    true -> 
							  ets:insert(Rttb, LastRtd#rtd{time=Time, quality= 1 })
						  end,
						  LastQua - 1;
					  false ->
						  ets:insert(Rttb, LastRtd#rtd{time=Time, quality=LastQua}), 
						  LastQua
				end,
				ertdb_store_history:write(HisStory, Key, Time, Quality, Value, Config)
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
% quality: 1 | 0
check_quality(Quality) ->
	Quality == "1" .

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
		

			
	
	
	
	
	