%%%----------------------------------------------------------------------
%%% Created	: 2013-11-25
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_store_history).

-import(proplists, [get_value/3]).

-include("elog.hrl").
-include("ertdb.hrl").

-behavior(gen_server).

-export([start_link/1,
        read/4, lookup/2,
        write/4, write/5
		]).
		
-export([open/2, open/3]).		

-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(DAY, 86400). %3600).

-define(OPEN_MODES, [binary, raw, {read_ahead, 1024}]).

-define(RTDB_VER, <<"RTDB0001">>).

-record(state, {id, dir, tb, buffer, db, hdb}).

-record(rtd, {key, time, last, ref, row}).

-record(db, {name, index, data}).

start_link(Id) ->
    gen_server:start_link({local, name(Id)}, ?MODULE, [Id],
		[{spawn_opt, [{min_heap_size, 204800}]}]).
		
name(Id) ->
    list_to_atom("ertdb_store_history" ++ integer_to_list(Id)).			
	
write(Pid, Key, Time, Value) ->
	gen_server:cast(Pid, {write, Key, Time, Value}).	
		
write(Pid, Key, Time, Value, Config) ->
    gen_server:cast(Pid, {write, Key, Time, Value, Config}).
	
read(Pid, Key, Begin, End) ->
    gen_server:call(Pid, {read, Key, Begin, End}). 
	
lookup(Pid, Key) ->
	gen_server:call(Pid, {lookup, Key}). 	
		
init([Id]) ->
	{ok, Opts} = application:get_env(store),
	Dir = get_value(dir, Opts, "var/data"),
	Buffer = get_value(buffer, Opts, 20),
	{ok, DB} = open(Dir, Id),
    %schedule daily rotation
    sched_daily_rotate(),
	TB = ets:new(rttb_last, [set, {keypos, #rtd.key}]),
	{ok, #state{id=Id, dir=Dir, buffer=Buffer+random:uniform(Buffer), tb=TB, db=DB, hdb=dict:new()}}.

open(Dir, Id) ->
	open(Dir, Id, dbname(extbif:timestamp())).

open(Dir, Id, Name) ->
	?INFO("open file:~p", [Name]),
	DbDir = dbdir(Dir, Id),
	IdxRef = lists:concat([Id, '_', Name, '_index']),
	IdxFile = lists:concat([DbDir, '/', Name, '.idx']),
	DataFile = lists:concat([DbDir, '/', Name, '.data']),
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
	
open2(Dir, Id, Name) ->
	?INFO("open2 file:~p", [Name]),
	DbDir = dbdir(Dir, Id),
	IdxRef = lists:concat([Id, '_', Name, '_index']),
	IdxFile = lists:concat([DbDir, '/', Name, '.idx']),
	DataFile = lists:concat([DbDir, '/', Name, '.data']),
	case filelib:is_file(DataFile) of
		false ->
			filelib:ensure_dir(DataFile);
		true ->
			ok
	end,		
	case file:open(DataFile, [read | ?OPEN_MODES]) of
		{ok, DataFd} ->
			{ok, IdxRef} = dets:open_file(IdxRef, [{file, IdxFile}, {type, bag}]),
			case file:read(DataFd, 8) of
			    {ok, ?RTDB_VER} -> ok;
			    eof -> file:write(DataFd, ?RTDB_VER)
		    end,	
			{ok, #db{name=Name, index=IdxRef, data=DataFd} };
		{error, _}=Error ->
			Error	
	end.		

dbdir(Dir, Id) ->	
	lists:concat([Dir, "/", extbif:zeropad(Id)]).
	
dbname(Ts) ->
	(Ts div ?DAY).		
	

get_idx(Begin, End, #state{id=Id, dir=Dir, db=DB, hdb=HDB} = State) ->
	Today = extbif:timestamp(),	
	{CBegin, CEnd} = check_time(Begin, End, Today),
	BeginIdx = dbname(CBegin), 
	EndIdx = dbname(CEnd),
	TodayIdx = dbname(Today),
	
	?INFO("beginidx:~p, endidx:~p", [BeginIdx, EndIdx]),
	
	StoreDB = fun(Idx, {DBS, DictHDB}) ->
				case dict:find(Idx, HDB) of
					{ok, EDB} ->
						{[EDB|DBS], DictHDB};
					error ->
						case open2(Dir, Id, Idx) of
							{ok, NDB} -> 
								{[NDB|DBS], dict:store(Idx, NDB, DictHDB)};
							{error, _} -> 
								{DBS, DictHDB}
						end	
				end		
			end,
	
	if EndIdx+1 >= TodayIdx ->
    	{HisDB, NewHDB} = 
			lists:foldl(StoreDB, {[], HDB}, lists:seq(BeginIdx, TodayIdx-1)),
		{lists:reverse([DB|HisDB]), State#state{hdb=NewHDB} };
	true ->
    	{HisDB, NewHDB} = 
			lists:foldl(StoreDB, {[], HDB}, lists:seq(BeginIdx, EndIdx+1)),
		{lists:reverse(HisDB), State#state{hdb=NewHDB} }
	end.		
	

sched_daily_rotate() ->
    Now = extbif:timestamp(),
    NextDay = (Now div ?DAY + 1) * ?DAY,
    ?INFO("will rotate at: ~p", [extbif:datetime(NextDay)]),
    Delta = (NextDay + 1 - Now) * 1000,
    erlang:send_after(Delta, self(), rotate).

			
handle_call({lookup, Key}, _From, #state{tb=TB} = State) ->
	DataE = case ets:lookup(TB, Key) of
		[] -> [];
		[#rtd{row=Rows}] ->
			lists:reverse(Rows)
	end,
	{reply, {ok, DataE}, State};	
	
handle_call({read, Key, Begin, End}, _From, #state{tb=TB} = State) ->
    %beginIdx is bigger than endidx
    {DbInRange, NewState} = get_idx(Begin, End, State),
	
	?INFO("read_idx: ~s ~p", [Key, DbInRange]),	
    IdxList = [{Name, DataFd, [Idx || {_K, BeginT, Idx} <- dets:lookup(IdxRef, Key), BeginT =< End]} 
                    || #db{name=Name, index=IdxRef, data=DataFd} <- DbInRange],
		
    ?INFO("do read: ~p, ~p", [Key, IdxList]),
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
	
	DataListF = filter_data(lists:flatten(DataList), Begin, End),
    {reply, {ok, DataListF}, NewState};	

	
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.		
	
handle_cast({write, Key, Time, Value}, 
		#state{tb=TB, db=DB, buffer=Buffer} = State) ->	
	?INFO("his noconfig write key:~p, time:~p, value:~p", [Key, Time, Value]),	
	case ets:lookup(TB, Key) of
		[] ->
			Rtd =#rtd{key=Key,time=Time,last=Value,row=[{Time,Value}]},
			ets:insert(TB, Rtd);
		[#rtd{row=Rows}] ->	
			NewRtData = #rtd{key=Key,time=Time,last=Value},
			if length(Rows)+1 >= Buffer ->
				flush_to_disk(DB, Key, Rows),
				ets:insert(TB, NewRtData#rtd{row=[]});
			true ->
				ets:insert(TB, NewRtData#rtd{row=[{Time,Value}|Rows]})
			end
	end,		
	{noreply, State};	
		
		
handle_cast({write, Key, Time, Value, #rtk_config{compress=Compress, his_maxtime=Maxtime}=Config}, 
		#state{tb=TB, db=DB, buffer=Buffer} = State) ->
	?INFO("his write key:~p, time:~p, value:~p", [Key, Time, Value]),
	case ets:lookup(TB, Key) of
		[] -> 
			Ref = erlang:send_after(Maxtime * 1000, self(), {maxtime, Key}),
			Rtd =#rtd{key=Key,time=Time,last=Value,ref=Ref,row=[{Time,Value}]},
			ets:insert(TB, Rtd);
		[#rtd{time=LastTime, last=LastValue,ref=LastRef, row=Rows}] ->
			case check_compress(Compress) of
				true ->
					case check_store({last, LastTime, LastValue}, {new, Time, Value}, Config) of
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
				false ->
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
		
handle_info(rotate, #state{id=Id, dir=Dir, db=DB} = State) ->
    {ok, NewDB} = open(Dir, Id),
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
	[{Time, _}|_] = lists:reverse(Rows),
    Idx = {Key, Time, {Pos, size(Data)}},
    ?INFO("BeginT:~p, Pos: ~p, Size: ~p", [Time, Pos, size(Data)]),
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
	
check_compress(Compress) ->
	Compress == "1" .	


check_store({last, Lastime, _LastValue}, {new, Time, _Value}, #rtk_config{his_dev=undefined, his_mintime=Mintime, his_maxtime=Maxtime}) ->
	Interval = Time - Lastime,
	Rule = lists:concat(["&(> interval ", Mintime,")(< interval ", Maxtime, ")"]),
	judge([Rule], [{interval, Interval}]);

check_store({last, Lastime, LastValue}, {new, Time, Value}, #rtk_config{his_dev=Dev, his_mintime=Mintime, his_maxtime=Maxtime}) ->
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
	
	
check_time(Begin, End, Today) when Begin =< End ->
	if (Today =< Begin) ->
		?ERROR("Begin:~p", [extbif:datetime(Begin)]),
		throw(error_time);
	true ->
		if Today =< End ->
			{Begin, Today};
		true ->
			{Begin, End}
		end
	end;
check_time(Begin, End, Today) ->
	check_time(End, Begin, Today).	
						
						
filter_data(DataList, Begin, End) ->
	?INFO("begin-end:~p, data:~p", [{extbif:datetime(Begin), extbif:datetime(End)}, DataList]),
	lists:reverse(filter_data_end(lists:reverse(filter_data_begin(DataList, Begin)), End)).						
		
filter_data_begin([], _) ->
	[];		
filter_data_begin([{Time, Value}|DataList], Begin) when Begin =< Time ->
	[{Time, Value}|DataList];
filter_data_begin([_|DataList], Begin) ->	
	filter_data_begin(DataList, Begin).
	
filter_data_end([], _) ->
	[];	
filter_data_end([{Time, Value}|DataList], End) when End >= Time ->
	[{Time, Value}|DataList];	
filter_data_end([_|DataList], End) ->
	filter_data_end(DataList, End).	
	
	
			
			