%% Copyright (c) 2012-2016 Peter Morgan <peter.james.morgan@gmail.com>
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

-module(raft_mesh).
-behaviour(gen_server).


-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start_link/0]).
-export([stop/0]).
-export([terminate/2]).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).


init([]) ->
    {ok, Hostname} = inet:gethostname(),
    {ok, [{Name, _}]} = net_adm:names(),
    mdns:subscribe(advertisement),
    {ok, #{env => raft_config:environment(), host => Hostname, node => Name}}.


handle_call(_, _, State) ->
    {stop, error, State}.


handle_cast(stop, State) ->
    {stop, normal, State}.


handle_info({_, {mdns, advertisement}, #{advertiser := raft_tcp_advertiser, ttl := 0}}, State) ->
    %% TTL of zero is a node saying goodbye
    {noreply, State};

handle_info({_, {mdns, advertisement}, #{advertiser := raft_tcp_advertiser,
                                         env := Env,
                                         node := Node,
                                         host := Host}},
            #{env := Env,
              host := Host,
              node := Node} = State) ->
    %% don't mesh with ourselves
    {noreply, State};

handle_info({_, {mdns, advertisement}, #{advertiser := raft_tcp_advertiser,
                                         env := Env,
                                         port := Port,
                                         host := Host}},
            #{env := Env} = State) ->
    %% mesh with any node in that shares our enviromment
    raft_cluster:add(uri(Host, Port)),
    {noreply, State};

handle_info({_, {mdns, advertisement}, _}, State) ->
    %% ignore any other advertisements
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, _) ->
    gproc:goodbye().

uri(Host, Port) ->
    "http://" ++ Host ++ ":" ++ any:to_list(Port) ++ "/api/".
