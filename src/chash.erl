%%%----------------------------------------------------------------------
%%% File    : chash.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : consistent hash by sha1.
%%% Created : 24 Dec 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(chash).

-export([hash/1, max/0]).

%% @doc hash
hash(String) when is_list(String) ->
    <<Hash:160>> = crypto:hash(sha, String), %sha1:binstring(String),
    Hash.

max() ->
    (2 bsl 160) - 1.

