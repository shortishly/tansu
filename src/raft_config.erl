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
-export([can/1]).
-export([db_schema/0]).
-export([enabled/1]).
-export([endpoint/1]).
-export([environment/0]).
-export([minimum/1]).
-export([port/1]).
-export([sm/0]).
-export([timeout/1]).

can(advertise) ->
    envy(to_boolean, can_advertise, true);
can(discover) ->
    envy(to_boolean, can_discover, true);
can(mesh) ->
    envy(to_boolean, can_mesh, true).

enabled(debug) ->
    envy(to_boolean, debug, false).

sm() ->
    envy(to_atom, sm, raft_sm_mnesia_kv).

endpoint(server) ->
    envy(to_list, endpoint_server, "/api");
endpoint(client) ->
    envy(to_list, endpoint_server, "/client").

port(http) ->
    envy(to_integer, http_port, 80).

db_schema() ->
    envy(to_atom, db_schema, ram).

environment() ->
    envy(to_list, environment, "dev").

acceptors(http) ->
    envy(to_integer, http_acceptors, 100).

timeout(election_low) ->
    envy(to_integer, timeout_election_low, 1500);
timeout(election_high) ->
    envy(to_integer, timeout_election_high, 3000);
timeout(leader_low) ->
    envy(to_integer, timeout_leader_low, 500);
timeout(leader_high) ->
    envy(to_integer, timeout_leader_high, 1000);
timeout(kv_expiry) ->
    envy(to_integer, timeout_kv_expiry, 1000);
timeout(mnesia_wait_for_tables) ->
    envy(to_integer_or_atom, timeout_mnesia_wait_for_tables, infinity);
timeout(sync_send_event) ->
    envy(to_integer_or_atom, timeout_sync_send_event, infinity).


minimum(quorum) ->
    envy(to_integer, minimum_quorum, 3).

envy(To, Name, Default) ->
    envy:To(raft, Name, default(Default)).

default(Default) ->
    [os_env, app_env, {default, Default}].

