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

-module(raft_connection_association).
-export([delete/1]).
-export([ids/0]).
-export([new_or_existing/2]).
-export([pid_for/1]).
-export([size/0]).

-on_load(on_load/0).

-record(?MODULE, {
           id :: binary(),
           pid :: pid()
          }).

on_load() ->
    crown_table:reuse(?MODULE).


new_or_existing(Id, Pid) ->
    case ets:lookup(?MODULE, Id) of
        [#?MODULE{pid = Pid}] ->
            true;

        [#?MODULE{pid = Existing}] ->
            error_logger:error_report([{module, ?MODULE},
                                       {line, ?LINE},
                                       {id, Id},
                                       {pid, Pid},
                                       {existing, Existing}]),
            error(badarg, [Id, Pid]);

        [] ->
            ets:insert_new(?MODULE, r(Id, Pid)) orelse
                error(badarg, [Id, Pid])
    end.

delete(Id) ->
    ets:delete(?MODULE, Id).

pid_for(Id) ->
    [#?MODULE{pid = Pid}] = ets:lookup(?MODULE, Id),
    Pid.

ids() ->
    ordsets:from_list([Id || #?MODULE{id = Id} <- ets:match_object(?MODULE, r('_', '_'))]).


size() ->
    ets:info(?MODULE, size).

r(Id, Pid) ->
    #?MODULE{id = Id, pid = Pid}.
