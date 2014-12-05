%%%----------------------------------------------------------------------
%%% Created	: 2013-11-28
%%% author 	: hejin1026@gmail.com
%%%----------------------------------------------------------------------
-module(ertdb_util).

-compile([export_all]).

timestamp() ->
	{MegaSecs, Secs, _MicroSecs} = os:timestamp(),
	MegaSecs * 1000000 + Secs.

timestamp({{_Y,_M,_D}, {_H,_MM,_S}} = DateTime) ->
	Seconds = fun(D) -> calendar:datetime_to_gregorian_seconds(D) end,
	Seconds(DateTime) - Seconds({{1970,1,1},{0,0,0}}).
	
datetime() ->
    calendar:local_time().	

pinfo(Pid) ->
    Props = [registered_name, message_queue_len, memory,
        total_heap_size, heap_size, reductions],
	case process_info(Pid, Props) of
	undefined ->
		{{undefined, node()}, [undefined]};
	Info ->
		Name = proplists:get_value(registered_name, Info),
		{{Name, node()}, Info}
	end.	
			
cancel_timer('undefined') -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

name(Tag, Id) ->
	list_to_atom( lists:concat([ Tag, "_", Id]) ).	

incr(Name, Val) ->
    case get(Name) of
    undefined ->
        put(Name, Val);
    OldVal ->
        put(Name, OldVal+Val)
    end.
