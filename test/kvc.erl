%% Copyright (c) 2016 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(kvc).

-export([code_change/3]).
-export([delete/1]).
-export([get/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([set/2]).
-export([start/1]).
-export([stop/0]).
-export([terminate/2]).

start(Cluster) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [Cluster], []).

set(Key, Value) ->
    gen_server:call(?MODULE, {set, Key, Value}, infinity).

get(Key) ->
    gen_server:call(?MODULE, {get, Key}, infinity).

delete(Key) ->
    gen_server:call(?MODULE, {delete, Key}, infinity).

stop() ->
    gen_server:cast(?MODULE, stop).


init([Cluster]) ->
    {ok,
     #{cluster => lists:foldl(
                    fun
                        (Host, A) ->
                            {ok, Origin} = gun:open(Host, 80, #{transport => tcp}),
                            A#{Origin => #{monitor => erlang:monitor(process, Origin),
                                           host => Host}}
                    end,
                    #{},
                    Cluster),
       requests => #{}}}.



handle_call({set, Key, Value}, From, State) ->
    {noreply, do_set(Key, Value, From, State)};

handle_call({get, Key}, From, State) ->
    {noreply, do_get(Key, From, State)};

handle_call({delete, Key}, From, State) ->
    {noreply, do_delete(Key, From, State)};

handle_call(_, _, State) ->
    {stop, error, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({gun_up, _, _}, State) ->
    {noreply, State};

handle_info({gun_down, _, http, _, _, _}, State) ->
    {noreply, State};

handle_info({gun_error, _, Request, Error},  #{requests := Requests} = State) ->
    #{Request := #{from := From}} = Requests,
    gen_server:reply(From, {error, Error}),
    {noreply, State#{requests := maps:without([Request], Requests)}};

handle_info({gun_data, _, Request, fin, Data}, #{requests := Requests} = State) ->
    #{Request := #{from := From, partial := Partial, headers := Headers}} = Requests,
    gen_server:reply(From, {ok, decode(Headers, <<Partial/bytes, Data/bytes>>)}),
    {noreply, State#{requests := maps:without([Request], Requests)}};

handle_info({gun_data, _, Request, nofin, Data}, #{requests := Requests} = State) ->
    #{Request := #{partial := Partial} = Detail} = Requests,
    {noreply, State#{requests := Requests#{Request := Detail#{partial := <<Partial/bytes, Data/bytes>>}}}};

handle_info({gun_response, _, Request, nofin, StatusCode, Headers}, #{requests := Requests} = State) ->
    #{Request := Detail} = Requests,
    {noreply, State#{requests := Requests#{Request := Detail#{status => StatusCode, headers => Headers}}}};

handle_info({gun_response, _, Request, fin, 204, _}, #{requests := Requests} = State) ->
    #{Request := #{from := From}} = Requests,
    gen_server:reply(From, ok),
    {noreply, State#{requests := maps:without([Request], Requests)}};

handle_info({gun_response, _, Request, fin, 503, _}, #{requests := Requests} = State) ->
    #{Request := #{from := From}} = Requests,
    gen_server:reply(From, service_unavailable),
    {noreply, State#{requests := maps:without([Request], Requests)}}.


terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.


do_set(Key, Value, From, State) ->
    do_request(<<"POST">>, Key, Value, From, State).

do_get(Key, From, State) ->
    do_request(<<"GET">>, Key, From, State).

do_delete(Key, From, State) ->
    do_request(<<"DELETE">>, Key, From, State).

decode(Headers, Body) when is_list(Headers) ->
    decode(maps:from_list(Headers), Body);

decode(#{<<"content-type">> := <<"application/json">>}, JSON) ->
    jsx:decode(JSON, [return_maps]);
decode(#{}, Body) ->
    Body.


do_request(Method, Key, Value, From, #{requests := Requests} = State) ->
    State#{requests := Requests#{gun:request(
                                   pick_from(State),
                                   Method,
                                   ["/client/keys/", any:to_list(Key)],
                                   [{<<"content-type">>, <<"application/json">>}],
                                   [jsx:encode(Value)]) => #{from => From, partial => <<>>}}}.    


do_request(Method, Key, From, #{requests := Requests} = State) ->
    State#{requests := Requests#{gun:request(
                                   pick_from(State),
                                   Method,
                                   ["/client/keys/", any:to_list(Key)],
                                   []) => #{from => From, partial => <<>>}}}.
    

pick_from(#{cluster := Cluster}) ->
    Members = maps:keys(Cluster),
    lists:nth(random:uniform(length(Members)), Members).


