-module (boss_cache_adapter_redis).
-author('ecestari@gmail.com').
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1, terminate/1]).
-export([get/3, set/5, delete/3]).

-define(TIMEOUT, 5000).

start() ->
    ok.

start(_Options) ->
    ok.

stop(Conn) ->
    eredis:stop(Conn).

init(Options) ->
    CacheServerOpts = proplists:get_value(cache_servers, Options, []),
    Host = proplists:get_value(host, CacheServerOpts, "127.0.0.1"),
    Port = proplists:get_value(port, CacheServerOpts, 6379),
    Database = proplists:get_value(database, CacheServerOpts, 0),
    Password = proplists:get_value(password, CacheServerOpts, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep, CacheServerOpts, 100),
    ConnectTimeout = proplists:get_value(connect_timeout, CacheServerOpts, ?TIMEOUT),
    eredis:start_link(Host, Port, Database, Password, ReconnectSleep, ConnectTimeout).

terminate(Conn) ->
    stop(Conn).

get(Conn, Prefix, Key) ->
    case eredis:q(Conn, ["GET", term_to_key(Prefix, Key)]) of
        {ok, undefined} ->
            undefined;
        {ok, Bin} -> 
            binary_to_term(Bin)
    end.

set(Conn, Prefix, Key, Val, TTL) ->
    {ok, Res} = eredis:q(Conn, ["SETEX", term_to_key(Prefix, Key), TTL, term_to_binary(Val)]),
    Res.

delete(Conn, Prefix, Key) ->
    eredis:q(Conn, ["DELETE", term_to_key(Prefix, Key)]).

% internal
term_to_key(Prefix, Term) ->
    lists:concat([Prefix, ":", boss_cache:to_hex(erlang:md5(term_to_binary(Term)))]).
