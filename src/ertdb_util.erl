%%%----------------------------------------------------------------------
%%% Created	: 2013-11-28
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_util).

-compile([export_all]).

timestamp() ->
	{MegaSecs, Secs, _MicroSecs} = erlang:now(),
	MegaSecs * 1000000 + Secs.

timestamp({{_Y,_M,_D}, {_H,_MM,_S}} = DateTime) ->
	Seconds = fun(D) -> calendar:datetime_to_gregorian_seconds(D) end,
	Seconds(DateTime) - Seconds({{1970,1,1},{0,0,0}}).
	
datetime() ->
    calendar:local_time().	
	