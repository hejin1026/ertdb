%%%----------------------------------------------------------------------
%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb).

-behaviour(gen_server).

-export([start_link/0,
		config/1,
		insert/3,
		fetch/1, fetch/3,
		lookup/1,
		build_config/2
		]).

-export([init/1, 
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).
		
-record(state, {journal, store}).	

-include("elog.hrl").	
-include("ertdb.hrl").
		
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).		
    
config(Config) ->
	gen_server:call(ertdb, {config, Config}).
	
insert(Key, Time, Value) ->	
	gen_server:cast(ertdb, {insert, Key, Time, Value}).

fetch(Key) ->
	gen_server:call(ertdb, {fetch, Key}).	
	
fetch(Key, Begin, End) ->
	gen_server:call(ertdb, {fetch, Key, Begin, End}).	
		
	
init([]) ->
	ets:new(rtk_config, [set, named_table, {keypos, #rtk_config.key}]),
	{ok, #state{journal=ertdb_journal, store=ertdb_store_current}}.
	
handle_call({config, KConfig}, _From,  State) when is_record(KConfig, rtk_config)->
	ets:insert(rtk_config, KConfig),			
	{reply, ?SUCC, State};
	
handle_call({fetch, Key}, _From, State) ->
	Value = ertdb_store_current:read(ertdb_store_current, Key),
	{reply, Value, State};
	
handle_call({fetch, Key, Begin, End}, _From, #state{}=State) ->
	Values = ertdb_store_history:read(ertdb_store_history, Key, Begin, End),
	{reply, Values, State};			
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.
	

handle_cast({insert, Key, Time, Value}, #state{journal = Journal, store = Store} = State) ->
	ertdb_store_current:write(Store, Key, Time, Value),
    ertdb_journal:write(Journal, Key, Time, Value),
    {noreply, State};
	
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.
	
handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.	

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
lookup(Key) ->
	ets:lookup(rtk_config, Key).
	
	
build_config(Key, Config) ->
	Data = [list_to_tuple(string:tokens(Item, "="))||Item <- string:tokens(Config, ",")],
	IConfig = case lookup(Key) of
		[] -> #rtk_config{key=Key};
		[OldConfig] -> OldConfig
	end,	
	parse(Data, IConfig).	
	
parse([], RTK) ->
	RTK;	
parse([{"compress", Value}|Config], RTK) ->
	parse(Config, RTK#rtk_config{compress=Value});
parse([{"dev", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{dev=Value});
parse([{"maxtime", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{maxtime=extbif:to_integer(Value)});
parse([{"mintime", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{mintime=extbif:to_integer(Value)});
parse([{"his_dev", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{his_dev=Value});
parse([{"his_maxtime", Value}|Config], RTK) ->	
	parse(Config, RTK#rtk_config{his_maxtime=extbif:to_integer(Value)});
parse([{"his_mintime", Value}|Config], RTK) ->		
	parse(Config, RTK#rtk_config{his_mintime=extbif:to_integer(Value)});
parse([{Key, _Value}|_], _RTK) ->			
	throw({unsupport_key, Key}).
	