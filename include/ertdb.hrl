
-define(SUCC, 1).
-define(FAIL, 0).

%% Public types

-type reconnect_sleep() :: no_reconnect | integer().

-type option() :: {host, string()} | {port, integer()} | {database, string()} | {password, string()} | {reconnect_sleep, reconnect_sleep()}.
-type server_args() :: [option()].

-type return_value() :: undefined | binary() | [binary()].

-type pipeline() :: [iolist()].

-type channel() :: binary().

%% Continuation data is whatever data returned by any of the parse
%% functions. This is used to continue where we left off the next time
%% the user calls parse/2.
-type continuation_data() :: any().
-type parser_state() :: status_continue | bulk_continue | multibulk_continue.

-record(pstate, {
          state = undefined :: parser_state() | undefined,
          continuation_data :: continuation_data() | undefined
}).

-define(NL, <<"\r\n">>).
-define(NL2, "\r\n").

-define(SOCKET_OPTS, [binary, {active, once}, {packet, raw}, {reuseaddr, true}, {nodelay, false}, {delay_send, true}]).


-record(rtk_config, {key, quality="0", coef=1, offset=0, compress, 
					dev, maxtime=300, mintime=0, his_dev=-1, his_maxtime=600, his_mintime=0}).		

-record(real_data, {key, time, quality, data, value}).