%%%----------------------------------------------------------------------
%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb).

-behaviour(gen_server).

-export([start_link/0,
		config/2,
		insert/3,
		fetch/4
		]).

-export([init/1, 
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).
		
-record(state, {journal, store}).	

-include("elog.hrl").	
		
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).		
    
config(Key, Config) ->
	gen_server:call(ertdb, {config, Key, Config}).
	
insert(Key, Time, Value) ->	
	gen_server:call(ertdb, {insert, Key, Time, Value}).
	
fetch(Key, Type, Begin, End) ->
	gen_server:call(ertdb, {fetch, Key, Type, Begin, End}).	
		
	
init([]) ->
	{ok, #state{journal=ertdb_journal, store=ertdb_store_current}}.
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.
	
handle_cast({config, Key, Config}, #state{store = Store} = State) ->
	ertdb_store_current:config(Store, {Key, Config}),
	{noreply, State};			

handle_cast({insert, Key, Time, Value}, #state{journal = Journal, store = Store} = State) ->
	ertdb_store_current:write(Store, {Key, Time, Value}),
    ertdb_journal:write(Journal, {Key, Time, Value}),
    {noreply, State};
	
handel({fetch, Key, Type, Begin, End}, #state{}=State) ->
	case Type of
		current ->
			ertdb_store_current:fetch(Key, Begin, End);
		history ->
			ertdb_store_history:fetch(Key, Begin, End)
	end,
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

