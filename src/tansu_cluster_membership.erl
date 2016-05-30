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

-module(tansu_cluster_membership).
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
    gen_server:start_link({local, ?MODULE}, ?MODULE, members(), []).

stop() ->
    gen_server:cast(?MODULE, stop).

init([]) ->
    ignore;

init(Members) ->
    {ok, #{members => queue:from_list(Members), size => length(Members)}, tansu_config:timeout(cluster_add_member)}.

handle_call(_, _, State) ->
    {stop, error, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(timeout, #{members := Members, size := Size} = State) ->
    case tansu_consensus:info() of
        #{connections := Connections} when map_size(Connections) + 1 == Size ->
            {noreply, State, tansu_config:timeout(cluster_add_member)};

        _ ->
            {{value, Member}, Remaining} = queue:out(Members),
            tansu_consensus:add_server(url(Member)),
            {noreply, State#{members := queue:in(Member, Remaining)}, tansu_config:timeout(cluster_add_member)}
    end.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

members() ->
    string:tokens(tansu_config:cluster(members), ",").

url(Member) ->
    "http://" ++ Member ++ tansu_config:endpoint(server).
