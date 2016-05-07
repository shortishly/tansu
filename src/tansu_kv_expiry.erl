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

-module(tansu_kv_expiry).
-behaviour(gen_server).

-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([start_link/0]).
-export([terminate/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->    
    {ok, undefined, tansu_config:timeout(kv_expiry)}.

handle_call(_, _, State) ->
    {stop, error, State}.

handle_cast(_, State) ->
    {stop, error, State}.

handle_info(timeout, State) ->
    case tansu_consensus:expired() of
        {ok, Expired} ->
            lists:foreach(
              fun
                  ({Category, Key}) ->
                      tansu_consensus:ckv_delete(Category, Key)
              end,
              Expired),
            {noreply, State, tansu_config:timeout(kv_expiry)};

        {error, not_leader} ->
            {noreply, State, tansu_config:timeout(kv_expiry)}
    end.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.
