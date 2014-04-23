%%%----------------------------------------------------------------------
%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb).

-behaviour(gen_server2).

-export([start_link/1,
		config/2,
		insert/3,
		fetch/1, fetch/3,
		lookup/1, lookup_his/1, test/1,
		name/1
		]).

-export([init/1, 
        handle_call/3, priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).
		
-record(state, {rtk_config, journal, cur_store, his_store}).	

-include("elog.hrl").	
-include("ertdb.hrl").
		
start_link(Id) ->
    gen_server:start_link({local, name(Id)}, ?MODULE, [Id], []).		
	
name(Id) ->
	list_to_atom("ertdb_" ++ integer_to_list(Id)).	
	
test(Key) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server:call(Pid, {exit, Key}).	
	
lookup(Key) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server:call(Pid, {lookup, Key}).		

lookup_his(Key) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server:call(Pid, {lookup_his, Key}).		    
	
config(Key, Config) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server:call(Pid, {config, Key, Config}).
	
insert(Key, Time, Value) ->	
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server:cast(Pid, {insert, Key, Time, Value}).



fetch(Key) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server2:call(Pid, {fetch, Key}).	
	
fetch(Key, Begin, End) ->
	Pid = chash_pg:get_pid(?MODULE, Key),
	gen_server2:call(Pid, {fetch, Key, Begin, End}).	
		
	
init([Id]) ->
	process_flag(trap_exit, true),
    %start store process
	{ok, HisStore} = ertdb_store_history:start_link(Id),
    {ok, CurStore} = ertdb_store_current:start_link(HisStore, Id),

    %start journal process
    {ok, Journal} = ertdb_journal:start_link(Id),

	{ok, Opts} = application:get_env(rtdb),
	VNodes = proplists:get_value(vnodes, Opts, 40),
    chash_pg:create(?MODULE),
    chash_pg:join(?MODULE, self(), name(Id), VNodes),
	
	RtkConfig = ets:new(rtk_config, [set, {keypos, #rtk_config.key}]),
	{ok, #state{rtk_config=RtkConfig, journal=Journal, cur_store=CurStore, his_store=HisStore}}.
	
handle_call({lookup, Key}, _From, #state{rtk_config=RtkConfig}=State) ->
	{reply, ets:lookup(RtkConfig, Key), State};	
	
handle_call({lookup_his, Key}, _From, #state{his_store=HisStore}=State) ->
	Values = ertdb_store_history:lookup(HisStore, Key),
	{reply, Values, State};
	
handle_call({config, Key, Config}, _From,  #state{rtk_config=RtkConfig}=State) ->
	Rest = 
		try 
			KConfig = build_config(RtkConfig, Key, binary_to_list(Config)),
			ets:insert(RtkConfig, KConfig),
			?SUCC
	    catch
	        Type:Error -> 
				?ERROR("error handle req:~p, ~p, ~p", [Type, Error, erlang:get_stacktrace()]),
				{error, "build fail"}
	    end,
	{reply, Rest, State};
		
handle_call({fetch, Key}, _From, #state{rtk_config=RtkConfig, cur_store=CurStore}=State) ->
	%% {ok, no_key} | {ok, #real_data}
 	Value = case ertdb_store_current:read(CurStore, Key) of
		{ok, no_key} ->
			case ets:lookup(RtkConfig, Key) of
				[] -> 
					{ok, no_init};	
				[_config] ->
					{ok, no_key}
			end;
		Other ->
			Other
	end,			
	{reply, Value, State};
	
handle_call({fetch, Key, Begin, End}, _From, #state{his_store=HisStore}=State) ->
	%% {ok, []} | {ok, [{Time, Quality, Value}|_]}
	Values = ertdb_store_history:read(HisStore, Key, Begin, End),
	{reply, Values, State};			
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.
	

handle_cast({insert, Key, Time, Value}, #state{journal=Journal, cur_store=CurStore} = State) ->
	ertdb_store_current:write(CurStore, Key, Time, Value),
    ertdb_journal:write(Journal, Key, Time, Value),
    {noreply, State};
	
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

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
	
build_config(RtkConfig, Key, Config) ->
	Data = [list_to_tuple(string:tokens(Item, "="))||Item <- string:tokens(Config, ",")],
	IConfig = case ets:lookup(RtkConfig, Key) of
		[] -> #rtk_config{key=Key};
		[OldConfig] -> OldConfig
	end,
	parse(Data, IConfig).
	
parse([], RTK) ->
	RTK;	
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
parse([{Key, _Value}|_], _RTK) ->			
	throw({unsupport_key, Key}).
	