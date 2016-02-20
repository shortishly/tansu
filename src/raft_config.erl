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

-module(raft_config).
-export([acceptors/1]).
-export([db_schema/0]).
-export([election_low_timeout/0]).
-export([election_high_timeout/0]).
-export([leader_low_timeout/0]).
-export([leader_high_timeout/0]).
-export([port/1]).

port(http) ->
    list_to_integer(
      raft:get_env(http_port, [os_env, app_env, {default, "80"}])).

db_schema() ->
    list_to_atom(
      raft:get_env(db_schema, [os_env, app_env, {default, "ram"}])).


acceptors(http) ->
    100.


election_low_timeout() ->
    1500.

election_high_timeout() ->
    3000.

leader_low_timeout() ->
    500.

leader_high_timeout() ->
    1000.
