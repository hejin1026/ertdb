%%%----------------------------------------------------------------------
%%% File    : extbif.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Extended BIF
%%% Created : 08 Dec 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(extbif).

-export([datetime/0,
        datetime/1,
        timestamp/0, 
		timestamp/1,
		strfdate/1,
		strfdate/2,
        strftime/0,
        strftime/1,
        microsecs/0,
        millsecs/0,
        to_atom/1,
        to_list/1, 
        to_binary/1, 
        binary_to_atom/1, 
        atom_to_binary/1,
        binary_split/2,
		binary_join/2,
        to_integer/1,
        zeropad/1,
		is_string/1,
		sleep/1]).

timestamp() ->
	{MegaSecs, Secs, _MicroSecs} = erlang:now(),
	MegaSecs * 1000000 + Secs.

%%% TODO g(now) - g(1970) = U + 8小时差
timestamp({{_Y,_M,_D}, {_H,_MM,_S}} = DateTime) ->
	Seconds = fun(D) -> calendar:datetime_to_gregorian_seconds(D) end,
	Seconds(DateTime) - Seconds({{1970,1,1},{8,0,0}}).

microsecs() ->
    {Mega,Sec,Micro} = erlang:now(),
    (Mega*1000000+Sec)*1000000+Micro.

millsecs() ->
    microsecs() div 100.

datetime() ->
    calendar:local_time().

strfdate({Y,M,D}) ->
    lists:concat([zeropad(I) || I <- [Y,M,D]]).

strfdate({Y,M,D}, Sep) when is_list(Sep) ->
    lists:concat([zeropad(I) || I <- [Y,M,D], Sep]).

strftime() ->
    strftime({date(), time()}).

strftime({{Y,M,D}, {H,MM,S}}) ->
    Date = string:join([zeropad(I) || I <- [Y,M,D]], "-"),
    Time = string:join([zeropad(I) || I <- [H, MM, S]], ":"),
    lists:concat([Date, " ", Time]).
    
%%% TODO g(now) = g(1970) + U + 8小时差	
datetime(Seconds) when is_integer(Seconds) ->
    BaseDate = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    Universal = calendar:gregorian_seconds_to_datetime(BaseDate + Seconds),
    calendar:universal_time_to_local_time(Universal).

to_atom(L) when is_list(L) -> 
    list_to_atom(L);
to_atom(B) when is_binary(B) ->
    to_atom(binary_to_list(B));
to_atom(A) when is_atom(A) ->
    A.

to_list(L) when is_list(L) ->
    L;

to_list(A) when is_atom(A) ->
    atom_to_list(A);

to_list(L) when is_integer(L) ->
    integer_to_list(L);

to_list(L) when is_float(L) ->
    string:join(io_lib:format("~.2f", [L]),"");

to_list(B) when is_binary(B) ->
    binary_to_list(B).


to_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
to_binary(B) when is_binary(B) ->
    B;
to_binary(I) when is_integer(I) ->
    integer_to_binary(I));
to_binary(I) when is_float(I) ->
    float_to_binary(I, [{decimals, 4}, compact]);
to_binary(L) when is_list(L) ->
    list_to_binary(L);
to_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
to_binary(N) ->
    N.

to_integer(I) when is_integer(I) ->
    I;
to_integer(I) when is_list(I) ->
    case string:str(I, ".") of
        0 ->
           {Value0 ,_} = string:to_integer(I),
           Value0;
        _ ->
            {Value0 ,_} = string:to_float(I),
             Value0
    end;
 to_integer(I) when is_binary(I) ->
     list_to_integer(binary_to_list(I));
 to_integer(I) ->
    %TODO: hide errors, should throw exception.
     I.

atom_to_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A)).

binary_to_atom(B) ->
    list_to_atom(binary_to_list(B)).

binary_split(<<>>, _C) -> [];
binary_split(B, C) -> binary_split(B, C, <<>>, []).

binary_split(<<C, Rest/binary>>, C, Acc, Tokens) ->
    binary_split(Rest, C, <<>>, [Acc | Tokens]);
binary_split(<<C1, Rest/binary>>, C, Acc, Tokens) ->
    binary_split(Rest, C, <<Acc/binary, C1>>, Tokens);
binary_split(<<>>, _C, Acc, Tokens) ->
    lists:reverse([Acc | Tokens]).

binary_join([], _Sep) ->
	<<>>;
binary_join(List, Sep) when is_list(Sep) ->
	binary_join(List, list_to_binary(Sep));

binary_join([H|T], Sep) when is_binary(Sep) ->
	Rest = lists:foldr(fun(E, Acc) -> 
		<<Sep/binary, E/binary, Acc/binary>>
	end, <<>>, T),
	<<H/binary, Rest/binary>>.
	
zeropad(I) when I < 10 ->
    lists:concat(["0", I]);
zeropad(I) ->
    integer_to_list(I).

is_string([C|Rest]) when C >= 0, C =< 255 -> is_string(Rest);

is_string([]) -> true;

is_string(_) -> false.

sleep(Time) ->
	receive 
	after Time -> 
		true	
	end.
