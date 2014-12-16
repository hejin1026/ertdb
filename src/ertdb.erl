%%%----------------------------------------------------------------------
%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb).

-behaviour(gen_server2).

-export([start_link/1, info/1,
		config/2,
		insert/3,
		fetch/1, fetch/3, read/2,
		lookup/1, lookup_his/1, test/1,
		name/1
		]).

-export([init/1, 
        handle_call/3, priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).
		
-record(state, {rtk_config, rttb, journal, his_store}).	

-record(rtd, {key, time, quality=5, data, value, ref}).

-include("elog.hrl").	
-include("ertdb.hrl").
		
start_link(Id) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id], []).		
	
name(Id) ->
	ertdb_util:name("ertdb", Id).		
	
test(Key) ->
	Pid = get_pid(Key),
	gen_server2:call(Pid, {exit, Key}).	

info(Type) ->
    Pids = chash_pg:get_pids(ertdb),
    [gen_server2:call(Pid, {info, Type}) || Pid <- Pids].
	
lookup(Key) ->
	Pid = get_pid(Key),
	gen_server2:call(Pid, {lookup, Key}).		

lookup_his(Key) ->
	Pid = get_pid(Key),
	gen_server2:call(Pid, {lookup_his, Key}).		    
	
%% 	
config(Key, Config) ->
	Pid = get_pid(Key),
	gen_server2:call(Pid, {config, Key, Config}).
	
insert(Key, Time, Value) ->	
	Pid = get_pid(Key),
	gen_server2:cast(Pid, {insert, Key, Time, Value}).

fetch(Key) ->
	Pid = get_pid(Key),
	rpc:call(node(Pid), ertdb, read, [Pid, Key]).
		
	
fetch(Key, Begin, End) ->
	Pid = get_pid(Key),
	{ok, DataList} = rpc:call(node(Pid), ertdb_store_history, read, [Pid, Key, Begin, End]),
	DataListF = ertdb_store_history:filter_data(lists:flatten(DataList), Begin, End),
	{ok, DataListF}.
	
get_pid(Key) ->
	case ets:lookup(ckey, Key) of
		[] -> 
			Pid = chash_pg:get_pid(?MODULE, Key),
			ertdb_server:insert(Key, Pid),
			Pid;
		[{_, Pid}] ->
			Pid
	end.			
	
	
read(Pid, Key) ->
	case ertdb_server:lookup(Pid) of
		[] -> 
			{error, nopid};
		[{_, Id, _}] ->
			?INFO("pid:~p, self:~p",[Pid, self()]),
		 	case handle_read(Key, ertdb_util:name("ertdb_rttb", Id)) of
				{ok, no_key} ->
					case ets:lookup(ertdb_util:name("ertdb_rtk_config", Id), Key) of
						[] -> 
							{ok, invalid_key};	
						[_config] ->
							{ok, no_key}
					end;
				Other ->
					Other
			end
	end.	
		
	
init([Id]) ->
	process_flag(trap_exit, true),
	RtkConfig = ets:new(ertdb_util:name("ertdb_rtk_config", Id), 
		[set, {keypos, #rtk_config.key}, named_table,{read_concurrency, true}]),
	RTTB = ets:new(ertdb_util:name("ertdb_rttb", Id), [set, {keypos, #rtd.key}, named_table]),
    %start store process
	{ok, HisStore} = ertdb_store_history:start_link(Id, RtkConfig),

    %start journal process
    {ok, Journal} = ertdb_journal:start_link(Id),

	{ok, Opts} = application:get_env(rtdb),
	VNodes = proplists:get_value(vnodes, Opts, 40),
    chash_pg:create(?MODULE),
    chash_pg:join(?MODULE, self(), name(Id), VNodes),
	
	ertdb_server:register(self(), Id, HisStore),
	{ok, #state{rtk_config=RtkConfig, rttb=RTTB, journal=Journal, his_store=HisStore}}.
	
	
handle_call({info, Type}, _From, #state{journal=Journal, his_store=HisStore} = State) ->
	Reply = case Type of
		"ertdb" -> ertdb_util:pinfo(self());
		"jour" -> ertdb_util:pinfo(Journal);
		"hist" -> ertdb_util:pinfo(HisStore);
		_ -> []
     end,
    {reply, Reply, State};	
	
	
handle_call({lookup, Key}, _From, #state{rtk_config=RtkConfig}=State) ->
	{reply, ets:lookup(RtkConfig, Key), State};		
	
handle_call({lookup_his, Key}, _From, #state{his_store=HisStore}=State) ->
	Values = ertdb_store_history:lookup(HisStore, Key),
	{reply, Values, State};
	
handle_call({config, Key, Config}, _From,  #state{rtk_config=RtkConfig}=State) ->
	?INFO("config:~p,~p", [Key, Config]),
	Rest = 
		try 
			KConfig = build_config(RtkConfig, Key, binary_to_list(Config)),
			case KConfig of
				delete ->
					ets:delete(RtkConfig, Key);
				_ ->	
					ets:insert(RtkConfig, KConfig)
			end,		
			?SUCC
	    catch
	        Type:Error -> 
				?ERROR("error handle req:~p, ~p, ~p", [Type, Error, erlang:get_stacktrace()]),
				{error, "build fail"}
	    end,
	{reply, Rest, State};
		
handle_call({fetch, Key}, _From, #state{rttb=Rttb, rtk_config=RtkConfig}=State) ->
	%% {ok, no_key} | {ok, #real_data}
 	Value = case handle_read(Key, Rttb) of
		{ok, no_key} ->
			case ets:lookup(RtkConfig, Key) of
				[] -> 
					{ok, invalid_key};	
				[_config] ->
					{ok, no_key}
			end;
		Other ->
			Other
		end,		
	{reply, Value, State};
	
handle_call({fetch, Key, Begin, End}, _From, #state{his_store=HisStore}=State) ->
	%% {ok, []} | {ok, [{Time, Quality, Value}|_]}
	Values = try 
				ertdb_store_history:read(HisStore, Key, Begin, End) 
			catch _:Reason ->
				?ERROR("fetch his timeout:~p,~p,~p", [Key, {Begin, End}, erlang:get_stacktrace()]),
				{error, timeout}	
			end,	
	{reply, Values, State};			
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.
	

handle_cast({insert, Key, Time, Value}, #state{journal=Journal} = State) ->
	handle_write(Key, Time, Value, State),
	ertdb_journal:write(Journal, Key, Time, Value),
    {noreply, State};
	
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.
	
handle_info({maxtime, Key}, #state{rttb=Rttb, his_store=HisStory, rtk_config=RtkConfig} = State) ->
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

handle_info({'EXIT', Pid, Reason}, State) ->
	?ERROR("unormal exit message received: ~p, ~p", [Pid, Reason]),
	{noreply, State};
	
handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.	

terminate(_Reason, _State) ->
	chash_pg:leave(?MODULE, self()),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

priorities_call({lookup, _}, _From, _State) ->
    10;
priorities_call({fetch, _}, _From, _State) ->
    8;
priorities_call(_, _From, _State) ->
    0.
	
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------	
handle_read(Key, Rttb) ->
	case ets:lookup(Rttb, Key) of
		[] -> 
			{ok, no_key};
		[#rtd{time=Time, quality=Quality, data=Data, value=Value}] ->
			{ok, #real_data{key=Key, time=Time, quality=Quality, data=Data, value=Value}}
	end.


handle_write(Key, Time, Data, #state{rttb=Rttb, his_store=HisStory, rtk_config=RtkConfig}) ->
	?INFO("cur write:~p, ~p, ~p", [Key, Time, Data]),
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
						?INFO("cur pass data:~p", [{Key, Time, Value}]),
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
								?INFO("cur filte data:~p", [{Key, Time, Value}]),
								ok
						end
				    end	
			end			
	end.


	
build_config(RtkConfig, Key, Config) ->
	Data = [list_to_tuple(string:tokens(Item, "="))||Item <- string:tokens(Config, ",")],
	IConfig = case ets:lookup(RtkConfig, Key) of
		[] -> #rtk_config{key=Key};
		[OldConfig] -> OldConfig
	end,
	parse(Data, IConfig).
	
parse([], RTK) ->
	RTK;	
parse([{"vaild", Value}|Config], RTK) ->
	case Value of	
		"1" ->
			parse(Config, RTK);	
		"0" ->
			delete	
	end;
parse([{"quality", Value}|Config], RTK) ->
	parse(Config, RTK#rtk_config{quality=Value});						
parse([{"coef", Value}|Config], RTK) ->
	parse(Config, RTK#rtk_config{coef=extbif:to_integer(Value)});		
parse([{"offset", Value}|Config], RTK) ->
	parse(Config, RTK#rtk_config{offset=extbif:to_integer(Value)});
parse([{"compress", Value}|Config], RTK) ->
	parse(Config, RTK#rtk_config{compress=Value});
parse([{"dev", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{dev=extbif:to_integer(Value)});
parse([{"maxtime", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{maxtime=extbif:to_integer(Value)});
parse([{"mintime", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{mintime=extbif:to_integer(Value)});
parse([{"his_dev", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{his_dev=extbif:to_integer(Value)});
parse([{"his_maxtime", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{his_maxtime=extbif:to_integer(Value)});
parse([{"his_mintime", Value}|Config], RTK) ->		
	parse(Config, RTK#rtk_config{his_mintime=extbif:to_integer(Value)});
parse([{_Key, _Value}|Config], RTK) ->			
	parse(Config, RTK).

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
