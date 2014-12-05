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
	% [ok = application:start(App) || App <- [crypto, lager]],
	ok = application:start(crypto),
	ok = application:start(compiler),
	ok = application:start(syntax_tools),
	ok = application:start(goldrush),
	ok = application:start(lager),
	% ok = application:start(esockd),
    {ok, SupPid} = ertdb_sup:start_link(),
    true = register(ertdb_app, self()),
    {ok, SupPid}.
	
stop(_State) ->
	ok.		