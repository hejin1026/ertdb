%%%----------------------------------------------------------------------
%%% File    : elog.hrl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Elog header
%%% Created : 31 Mar 2009
%%% License : http://www.opengoss.com/
%%%
%%% Copyright (C) 2007-2009, www.opengoss.com
%%%----------------------------------------------------------------------

%% ---------------------------------
%% Logging mechanism

-define(PRINT(Format, Args),
    io:format(Format, Args)).

-define(PRINT_MSG(Msg),
    io:format(Msg)).

-define(DEBUG(Format, Args),
    lager:debug(Format, Args)).

-define(DEBUG_TRACE(Dest, Format, Args),
    lager:debug(Dest, Format, Args)).

-define(DEBUG_MSG(Msg),
    lager:debug(Msg)).

-define(INFO(Format, Args),
    lager:info(Format, Args)).

-define(INFO_TRACE(Dest, Format, Args),
    lager:info(Dest, Format, Args)).

-define(INFO_MSG(Msg),
    lager:info(Msg)).

-define(WARN(Format, Args),
    lager:warning(Format, Args)).

-define(WARN_TRACE(Dest, Format, Args),
    lager:warning(Dest, Format, Args)).

-define(WARN_MSG(Msg),
    lager:warning(Msg)).
			      
-define(WARNING(Format, Args),
    lager:warning(Format, Args)).

-define(WARNING_TRACE(Dest, Format, Args),
    lager:warning(Dest, Format, Args)).

-define(WARNING_MSG(Msg),
    lager:warning(Msg)).

-define(ERROR(Format, Args),
    lager:error(Format, Args)).

-define(ERROR_TRACE(Dest, Format, Args),
    lager:error(Dest, Format, Args)).

-define(ERROR_MSG(Msg),
    lager:error(Msg)).

-define(CRITICAL(Format, Args),
    lager:critical(Format, Args)).

-define(CRITICAL_TRACE(Dest, Format, Args),
    lager:critical(Dest, Format, Args)).

-define(CRITICAL_MSG(Msg),
    lager:critical(Msg)).

