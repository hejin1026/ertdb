%%%----------------------------------------------------------------------
%%% Created	: 2013-11-21
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_journal).

-import(extbif, [timestamp/0, zeropad/1]).

-import(proplists, [get_value/2, get_value/3]).

-include("elog.hrl").

-behavior(gen_server).

-export([start_link/0, 
        write/2]).

-export([init/1, 
        handle_call/3, 
        handle_cast/2,
        handle_info/2,
        priorities_info/2,
        terminate/2,
        code_change/3]).

-record(state, {logdir, logfile}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [],
                [{spawn_opt, [{min_heap_size, 20480}]}]).

write(Pid, {Key, Time, Value}) ->
    gen_server2:cast(Pid, {write, Key, Time, Value}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    put(commit_count, 0),
    put(commit_time, 0),
	random:seed(now()),
    {ok, Opts} = application:get_env(journal),
    Dir = get_value(dir, Opts),
    State = #state{logdir = Dir},
    {noreply, NewState} = handle_info(journal_rotation, State),
    ?INFO("~p is started.", [ertdb_journal]),
    {ok, NewState}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {reply, {error, badreq}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({write, Key, Time, Value}, #state{logfile = LogFile} = State) ->
	handle_write(LogFile, {Key, Time, Value}),
	{noreply, State};

    
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(journal_rotation, #state{logdir = Dir, logfile = File} = State) ->
    close_file(File),
    Now = timestamp(),
    {Hour,_,_} = time(),
    FilePath = lists:concat([Dir, "/", extbif:strfdate(date()), "/", 
		zeropad(Hour), "/",  "ertdb.journal"]),
    filelib:ensure_dir(FilePath),
    {ok, NewFile} = file:open(FilePath, [write]),
    NextHour = ((Now div 3600) + 1) * 3600,
    erlang:send_after((NextHour + 60 - Now) * 1000, self(), journal_rotation),
    {noreply, State#state{logfile = NewFile}};

handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.


priorities_info(journal_rotation, _State) ->
    10;
priorities_info(_, _State) ->
	0.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, #state{logfile = LogFile}) ->
    close_file(LogFile),
    ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
handle_write(undefined, _) ->
	ok;
handle_write(LogFile, {Key, Time, Value}) ->
	file:write(LogFile, line({Key, Time, Value})).

line({Key, Time, Value}) ->
    list_to_binary([Key, "@", integer_to_list(Time), ":", Value, "\n"]).	

close_file(undefined) ->
    ok;
close_file(File) ->
    file:close(File).



