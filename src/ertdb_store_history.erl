%%%----------------------------------------------------------------------
%%% Created	: 2013-11-25
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_history).

-import(proplists, [get_value/3]).

-include("elog.hrl").
-include("ertdb.hrl").

-behavior(gen_server).

-export([start_link/0,
        read/4, lookup/2,
        write/5
		]).
		
-export([open/1]).		

-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(DAY, 86400). %3600).

-define(OPEN_MODES, [binary, raw, {read_ahead, 1024}]).

-define(RTDB_VER, <<"RTDB0001">>).

-record(state, {dir, tb, buffer, db}).

-record(rtd, {key, time, last, ref, row}).

-record(db, {name, index, data}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
write(Pid, Key, Time, Value, Config) ->
    gen_server:cast(Pid, {write, Key, Time, Value, Config}).
	
read(Pid, Key, Begin, End) ->
    gen_server:call(Pid, {read, Key, Begin, End}). 
	
lookup(Pid, Key) ->
	gen_server:call(Pid, {lookup, Key}). 	
		
init([]) ->
	{ok, Opts} = application:get_env(store),
	Dir = get_value(dir, Opts, "var/data"),
	Buffer = get_value(buffer, Opts, 20),
	{ok, DB} = open(Dir),
    %schedule daily rotation
    sched_daily_rotate(),
	TB = ets:new(rttb_last, [set, {keypos, #rtd.key}]),
	{ok, #state{dir=Dir, buffer=Buffer+random:uniform(Buffer), tb=TB, db=DB}}.

open(Dir) ->
	open(Dir, dbname(extbif:timestamp())).

open(Dir, Name) ->
	?INFO("open file:~p", [Name]),
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
	{ok, #db{name=Name, index=IdxRef, data=DataFd} }.

sched_daily_rotate() ->
    Now = extbif:timestamp(),
    NextDay = (Now div ?DAY + 1) * ?DAY,
    %?INFO("will rotate at: ~p", [ertdb_util:datetime(NextDay)]),
    Delta = (NextDay + 1 - Now) * 1000,
    erlang:send_after(Delta, self(), rotate).
	
dbname(Ts) ->
	integer_to_list(Ts div ?DAY).	

get_idx(Begin, End, #state{dir=Dir, db = DB}) ->
	Today = extbif:timestamp(),	
	CBegin = if Today > Begin ->
					Begin;
				true ->
					Today
				end,
	CEnd = if Today > End ->
					End;
				true ->
					Today
				end,
	BeginIdx = CBegin div ?DAY, 
	EndIdx = CEnd div ?DAY,
	?INFO("beginidx:~p, endidx:~p", [BeginIdx, EndIdx]),
    DbInRange = 
		lists:map(fun(Idx) ->
			{ok, NDB} = open(Dir, Idx), NDB
		end, lists:seq(BeginIdx, EndIdx-1)),
	LastDB = 
	if 	EndIdx == (Today div ?DAY) -> DB;
		true -> {ok, LDB} = open(Dir, EndIdx), LDB
	end,
	[LastDB|lists:reverse(DbInRange)].
			
handle_call({lookup, Key}, _From, #state{tb=TB} = State) ->
	DataE = case ets:lookup(TB, Key) of
		[] -> [];
		[#rtd{row=Rows}] ->
			lists:reverse(Rows)
	end,
	{reply, {ok, DataE}, State};	
	
handle_call({read, Key, Begin, End}, _From, #state{tb=TB} = State) ->
    %beginIdx is bigger than endidx
    DbInRange = get_idx(Begin, End, State),
	?INFO("read_idx: ~s ~p", [Key, DbInRange]),	
    IdxList = [{Name, DataFd, [Idx || {_K, Idx} <- dets:lookup(IdxRef, Key)]} 
                    || #db{name=Name, index=IdxRef, data=DataFd} <- DbInRange],
		
    % ?INFO("do read: ~p, ~p", [Key, IdxList]),
    DataF = lists:map(fun({Name, DataFd, Indices}) -> 
		% DataFile = lists:concat(["var/data", '/', Name, '.data']),		
		% {ok, DataFd} = file:open(DataFile, [read, write, append | ?OPEN_MODES]),
        case file:pread(DataFd, Indices) of
	        {ok, DataL} ->
				% ?INFO("get datal:~p", [DataL]),
	            {ok, [binary_to_term(Data) || Data <- DataL]};
	        eof -> 
	            {ok, []};
	        {error, Reason} -> 
	            ?ERROR("pread ~p error: ~p", [Name, Reason]),
	            {error, Reason}
        end
    end, [ E || {_, _, Indices} = E <- IdxList, Indices =/= []]),
	DataFl = [Rows || {ok, Rows} <- DataF],
	Today = extbif:timestamp(),	
	DataList =  
		if 	(End div ?DAY) == (Today div ?DAY) -> 
			DataE = case ets:lookup(TB, Key) of
				[] -> [];
				[#rtd{row=Rows}] ->
					lists:reverse(Rows)
			end,
			lists:append([DataFl, DataE]);
		true -> 
			DataFl
	end,
    {reply, {ok, lists:flatten(DataList)}, State};	

	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.		
	
		
handle_cast({write, Key, Time, Value, #rtk_config{compress=Compress, his_maxtime=Maxtime}=Config}, 
		#state{tb=TB, db=DB, buffer=Buffer} = State) ->
	?INFO("his write key:~p, time:~p, value:~p", [Key, Time, Value]),
	case ets:lookup(TB, Key) of
		[] -> 
			Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
			Rtd =#rtd{key=Key,time=Time,last=Value,ref=Ref,row=[{Time,Value}]},
			ets:insert(TB, Rtd);
		[#rtd{time=LastTime, last=LastValue,ref=LastRef, row=Rows}] ->
			case Compress of
				"1" ->
					case check({last, LastTime, LastValue}, {new, Time, Value}, Config) of
						true ->
							?INFO("his pass data:~p", [{new, Time, Value}]),
							cancel_timer(LastRef),			
							Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
							NewRtData = #rtd{key=Key,time=Time,last=Value,ref=Ref},
							if length(Rows)+1 >= Buffer ->
								flush_to_disk(DB, Key, Rows),
								ets:insert(TB, NewRtData#rtd{row=[]});
							true ->
								ets:insert(TB, NewRtData#rtd{row=[{Time,Value}|Rows]})
							end;	 	
						false ->
							?INFO("his filte data:~p", [{new, Time, Value}]),
							ok
					end;
				_ ->
					cancel_timer(LastRef),			
					Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
					NewRtData = #rtd{key=Key,time=Time,last=Value,ref=Ref},
					if length(Rows)+1 >= Buffer ->
						flush_to_disk(DB, Key, [{Time,Value}|Rows]),
						ets:insert(TB, NewRtData#rtd{row=[]});
					true ->
						ets:insert(TB, NewRtData#rtd{row=[{Time,Value}|Rows]})
					end
			end						
	end,
	{noreply, State};		
		
handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

handle_info({maxtime, Key}, #state{tb=TB, db=DB, buffer=Buffer} = State) ->
	[#rtk_config{his_maxtime=Maxtime}] = ertdb:lookup(Key),
	case ets:lookup(TB, Key) of
		[] ->
			throw({no_key, Key});
		[#rtd{last=Value, row=Rows}] ->
			?INFO("his maxtime data:~p", [{Key, Value}]),			
			Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
			Time = extbif:timestamp(),
			NewRtData = #rtd{key=Key,time=Time,last=Value,ref=Ref},
			if length(Rows)+1 >= Buffer ->
				flush_to_disk(DB, Key, [{Time,Value}|Rows]),
				ets:insert(TB, NewRtData#rtd{row=[]});
			true ->
				ets:insert(TB, NewRtData#rtd{row=[{Time,Value}|Rows]})
			end
	end,	
	{noreply, State};
		
handle_info(rotate, #state{dir=Dir, db=DB} = State) ->
    {ok, NewDB} = open(Dir),
    %close oldest db
    close(DB),
    %rotation 
    sched_daily_rotate(),
	{noreply, State#state{db=NewDB} };
		
	
handle_info(Info, State) ->
    ?ERROR("his badinfo: ~p", [Info]),
    {noreply, State}.	

terminate(_Reason, #state{db = DB}) ->
	close(DB),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
	
%%-----inter fun----%%	
flush_to_disk(#db{index=IdxRef,  data=DataFd}, Key, Rows) ->
    {ok, Pos} = file:position(DataFd, eof),
    Data = term_to_binary(lists:reverse(Rows), [compressed]),
    Idx = {Key, {Pos, size(Data)}},
    ?INFO("Pos: ~p, LastPos: ~p, Size: ~p", [Pos, Pos, size(Data)]),
    case file:write(DataFd, Data) of
    ok ->
        ?INFO("write indices: ~p", [Idx]),
        dets:insert(IdxRef, Idx);
    {error, Reason} ->
        ?ERROR("~p", [Reason])
    end.
	
close(undefined) -> ok;
close(#db{index=IdxRef, data=DataFd}) ->
    dets:close(IdxRef),
    file:close(DataFd).	


check({last, Lastime, _LastValue}, {new, Time, _Value}, #rtk_config{his_dev=undefined, his_mintime=Mintime, his_maxtime=Maxtime}) ->
	Interval = Time - Lastime,
	Rule = lists:concat(["&(> interval ", Mintime,")(< interval ", Maxtime, ")"]),
	judge([Rule], [{interval, Interval}]);

check({last, Lastime, LastValue}, {new, Time, Value}, #rtk_config{his_dev=Dev, his_mintime=Mintime, his_maxtime=Maxtime}) ->
	Interval = Time - Lastime,
	Rule = lists:concat(["&(> interval ", Mintime,")(< interval ", Maxtime, ")"]),
	Deviation = abs(extbif:to_integer(Value) - extbif:to_integer(LastValue)),
	Rule2 = lists:concat(["> deviation ", Dev]),	
	judge([Rule,Rule2], [{interval, Interval}, {deviation, Deviation}]).
		
	
judge([], _Data) ->	
	true;
judge([Rule|Rs], Data) ->
	{ok, Exp} = prefix_exp:parse(Rule),
	case prefix_exp:eval(Exp, Data) of
		true -> judge(Rs, Data);
		false -> 
			?INFO("judge false :~p, ~p",[Rule, Data]),
			false
	end.	
			
cancel_timer('undefined') -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

		
			