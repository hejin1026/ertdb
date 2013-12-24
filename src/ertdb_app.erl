%%% Created : 2013-11-20
%%% author : hejin1026@gmail.com.
%%%----------------------------------------------------------------------

-module(ertdb_app).

-behavior(application).

-export([start/0]).

-export([start/2, stop/1]).

start() ->
	application:start(ertdb).
	
start(_Type, _Args) ->
	[application:start(App) || App <- [sasl, crypto]],
    {ok, SupPid} = ertdb_sup:start_link(),
    true = register(ertdb_app, self()),
    {ok, SupPid}.
	
stop(_State) ->
	ok.		