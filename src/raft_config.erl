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
-export([port/1]).
-export([timeout/1]).

port(http) ->
    any:to_integer(get_env(http_port, 80)).

db_schema() ->
    any:to_atom(get_env(db_schema, ram)).

get_env(Key, Default) ->
    raft:get_env(Key, [os_env, app_env, {default, Default}]).



acceptors(http) ->
    100.


timeout(election_low) ->
    1500;
timeout(election_high) ->
    3000;
timeout(leader_low) ->
    500;
timeout(leader_high) ->
    1000.
