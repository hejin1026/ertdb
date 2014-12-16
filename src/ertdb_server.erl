
-module(ertdb_server).

-include("elog.hrl").

-export([start_link/0, lookup/1, register/3, unregister/1, insert/2]).

-behaviour(gen_server2).

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

-record(state, {}).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

size() ->
	ets:info(server, size).

register(CurrPid, Id, Pid) ->
    gen_server2:cast(?MODULE, {register, CurrPid, Id, Pid}).
	
lookup(CurrPid) ->
	gen_server:call(?MODULE, {lookup, CurrPid}).

unregister(CurrPid) ->
    gen_server2:cast(?MODULE, {unregister, CurrPid}).
	
insert(Key, Pid) ->
	gen_server2:cast(?MODULE, {insert, Key, Pid}).
						

%%----------------------------------------------------------------------------

init([]) ->
	ets:new(server, [set, named_table]),
	ets:new(ckey, [set, named_table]),
	?INFO("~p is started.", [?MODULE]),
    {ok, #state{}}. 

%%--------------------------------------------------------------------------
handle_call({lookup, CurrPid}, _From, State) ->
	Res = ets:lookup(server, CurrPid),
	{reply, Res, State};

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast({register, CurrPid, Id, Pid}, State) ->
	ets:insert(server, {CurrPid, Id, Pid}),
    {noreply, State};

handle_cast({unregister, CurrPid}, State) ->
	ets:delete(client, CurrPid),
	{noreply, State};
	
handle_cast({insert, Key, Pid}, State) ->
	ets:insert(ckey, {Key, Pid}),
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.
	
handle_info(Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

