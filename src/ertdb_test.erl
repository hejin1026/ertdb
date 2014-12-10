
-module(ertdb_test).

-include("elog.hrl").

-behavior(gen_server).

-export([start_link/0, go/2, config/2, stop/0]).

-export([init/1, 
		handle_call/3, 
		handle_cast/2, 
		handle_info/2, 
		terminate/2, 
		code_change/3]).

-record(state, {channel, num, interval}).	

		
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
	
stop() ->
	gen_server:cast(?MODULE, stop).		
	
	
go(Count, Step) ->
	lists:foreach(fun(Cid) ->
		?MODULE ! {send_data, [{id, Cid}, {step, Step}]},
		timer:sleep(1000)
	end, lists:seq(0, Count-1)).
	
config(Count, Step) ->	
	lists:foreach(fun(Cid) ->
		?MODULE ! {config, [{id, Cid}, {step, Step}]}
	end, lists:seq(0, Count-1)).	
			
init([]) ->
	process_flag(trap_exit, true),
    {ok, #state{num=10, interval=60}}.
		
	
handle_call(Req, _From, State) ->
    ?ERROR("Unexpected request: ~p", [Req]),
    {reply, ok, State}.	
	
handle_cast(stop, State) ->
 	{stop, no, State};	

handle_cast(Msg, State) ->
    ?ERROR("Unexpected message: ~p", [Msg]),
    {noreply, State}.

handle_info({config, Channel}, State=#state{num=Num} ) ->
	Cid = proplists:get_value(id, Channel),
	Step = proplists:get_value(step, Channel),
	lists:foreach(fun(C) ->
		lists:foreach(fun(No) ->
			Key = lists:concat([C, ":11:", No]),
			ertdb:config(list_to_binary(Key), <<"compress=1,dev=0.1,his_dev=0.2,his_maxtime=120">>)
		end, lists:seq(0, Num-1))
	end, lists:seq(Cid * Step + 1, (Cid+1) * Step)),    
	{noreply, State};

handle_info({send_data, Channel}, State=#state{num=Num, interval=Interval} ) ->
	?INFO("send_data:~p", [Channel]),
	Cid = proplists:get_value(id, Channel),
	Step = proplists:get_value(step, Channel),
	spawn(fun() ->
        try send_data(Cid * Step + 1, (Cid+1) * Step, Num) 
        catch
            _:Error ->
                ?ERROR("run_catch : ~p ~n Task: ~p ~n ~p", [Error, Channel, erlang:get_stacktrace()])
        end
	end),
	erlang:send_after(Interval * 1000, self(), {send_data, Channel}),
	{noreply, State};

handle_info({'EXIT', Pid, Reason}, State) ->
	?ERROR("unormal exit message received: ~p, ~p", [Pid, Reason]),
	{noreply, State};	

handle_info(Info, State) ->
	?INFO("Unexpected info received: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
send_data(StartCid, EndCid, Num) ->
	?INFO("cid:~p - ~p, num:~p", [StartCid, EndCid, Num]),
	lists:foreach(fun(C) ->
		lists:foreach(fun(No) ->
			Key = lists:concat([C, ":11:", No]),
			random:seed(os:timestamp()),
			Value = random:uniform(1000),
			ertdb:insert(list_to_binary(Key), extbif:timestamp(), extbif:to_binary(Value))
		end, lists:seq(0, Num-1))
	end, lists:seq(StartCid, EndCid)).
	
	
	
	
