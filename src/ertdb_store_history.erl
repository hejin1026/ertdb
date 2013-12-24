%%%----------------------------------------------------------------------
%%% Created	: 2013-11-25
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_history).

-import(extbif, [zeropad/1]).

-import(proplists, [get_value/3]).

-include("elog.hrl").

-behavior(gen_server).

-export([start_link/0,
        % read/4, 
        write/2
		]).

-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(DAY, 86400). %3600).

-define(OPEN_MODES, [binary, raw, {read_ahead, 1024}]).

-define(RTDB_VER, <<"RTDB0001">>).

-record(state, {dir, rttb, buffer, db}).

-record(rtd, {key, time, last, ref, row}).

-record(db, {index, data}).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
write(Pid, {Key, Time, Value, Config}) ->
    gen_server2:cast(Pid, {write, Key, Time, Value, Config}).
		
init([]) ->
	{ok, Opts} = application:get_env(store),
	Dir = get_value(dir, Opts, "var/data"),
	Buffer = get_value(buffer, Opts, 20),
	DB = open(Dir),
	RTTB = ets:new(rttb_last, [set, {keypos, #rtd.key}]),
	{ok, #state{dir=Dir, buffer=Buffer+random:uniform(Buffer), rttb = RTTB, db=DB}}.

open(Dir) ->
	Name = dbname(),
	IdxRef = lists:concat([Name, '_index']),
	IdxFile = lists:concat([Dir, '/', Name, '.idx']),
	DataFile = lists:concat([Dir, '/', Name, '.data']),
	case filelib:is_file(DataFile) of
		false ->
			filelib:ensure_dir(DataFile);
		true ->
			ok
	end,		
	{ok, IdxRef} = dets:open_file(IdxRef, [{file, IdxFile}, {type, bag}]),
	{ok, DataFd} = file:open(DataFile, [read, write, append | ?OPEN_MODES]),	
	case file:read(DataFd, 8) of
	    {ok, ?RTDB_VER} -> ok;
	    eof -> file:write(DataFd, ?RTDB_VER)
    end,
	{ok, #db{index=IdxRef, data=DataFd} }.
	
dbname() ->
	Ts = extbif:timestamp(),
	integer_to_list(Ts div ?DAY).	
	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.		
	
		
handle_cast({write, Key, Time, Value, Config}, #state{rttb=Rttb, db=DB, buffer=Buffer} = State) ->
	case ets:lookup(Rttb, Key) of
		[] -> 
			Maxtime = propslist:get_value(his_maxtime, Config),
			Ref = erlang:send_after(Maxtime, self(), {maxtime, Maxtime}),
			Rtd =#rtd{key=Key,time=Time,last=Value,ref=Ref,row=[{Time,Value}]},
			ets:insert(Rttb, Rtd);
		[#rtd{time=LastTime, last=LastValue,ref=LastRef, row=Rows}] ->
			case check({last, LastTime, LastValue}, {new, Time, Value}, Config) of
				true ->
					erlang:cancel_timer(LastRef),			
					Maxtime = propslist:get_value(his_maxtime, Config),
					Ref = erlang:send_after(Maxtime, self(), {maxtime, Key, Config}),
					NewRtData = #rtd{key=Key,time=Time,last=Value,ref=Ref,row=[{Time,Value}|Rows]},
					if length(Rows) >= Buffer ->
						flush_to_disk(DB, Key, Rows),
						ets:insert(Rttb, NewRtData#rtd{row=[]});
					true ->
						ets:insert(Rttb, NewRtData)
					end;	 	
				false ->
					ok
			end			
	end,
	{noreply, State};		
		
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

handle_info({maxtime, Key, Config}, #state{rttb = Rttb, db=DB, buffer=Buffer} = State) ->
	case ets:lookup(Rttb, Key) of
		[] ->
			throw({no_key, Key});
		[#rtd{last=Value, row=Rows}] ->			
			Maxtime = propslist:get_value(his_maxtime, Config),
			Ref = erlang:send_after(Maxtime, self(), {maxtime, Key, Config}),
			Time = ertdb_util:timestamp(),
			NewRtData = #rtd{key=Key,time=Time,last=Value,ref=Ref,row=[{Time,Value}|Rows]},
			if length(Rows) >= Buffer ->
				flush_to_disk(DB, Key, Rows),
				ets:insert(Rttb, NewRtData#rtd{row=[]});
			true ->
				ets:insert(Rttb, NewRtData)
			end
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
flush_to_disk(#db{index=IdxRef,  data=DataFd}, Key, Rows) ->
    {ok, Pos} = file:position(DataFd, eof),
    Data = term_to_binary(lists:reverse(Rows), [compressed]),
    Idx = {Key, {Pos, size(Data)}},
    %?INFO("Pos: ~p, LastPos: ~p, Size: ~p", [Pos, LastPos, size(Data)]),
    case file:write(DataFd, Data) of
    ok ->
        %?INFO("write indices: ~p", [Indices]),
        dets:insert(IdxRef, Idx);
    {error, Reason} ->
        ?ERROR("~p", [Reason])
    end.
	

check({last, Lastime, LastValue}, {new, Time, Value}, Config) ->
	Rule0 = "= has_cur 1",
	Interval = Time - Lastime,
	Rule = "&(> interval his_mintime)(< interval his_maxtime)",
	Deviation = abs(Value - LastValue),
	Rule2 = "> deviation his_deviation",	
	judge([Rule0, Rule,Rule2], [{interval, Interval}, {deviation, Deviation}|Config]).
		
	
judge([], _Data) ->	
	true;
judge([Rule|Rs], Data) ->
	{ok, Exp} = prefix_exp:parse(Rule),
	case prefix_exp:eval(Exp, Data) of
		true -> judge(Rs, Data);
		false -> false
	end.	
			
		
			