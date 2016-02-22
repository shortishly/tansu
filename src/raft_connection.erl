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

-module(raft_connection).
-export([associate/2]).
-export([broadcast/1]).
-export([delete/1]).
-export([ids/0]).
-export([new/2]).
-export([send/2]).
-export([size/0]).

-on_load(on_load/0).

-record(?MODULE, {
           pid :: pid(),
           id,
           sender
          }).

on_load() ->
    crown_table:reuse(?MODULE, ordered_set).


new(Pid, Sender) ->
    ets:insert_new(?MODULE, r(Pid, undefined, Sender)) orelse
        error(badarg, [Pid, Sender]).

delete(Pid) ->
    ets:delete(?MODULE, Pid).

associate(Pid, Id) ->
    ets:update_element(?MODULE, Pid, {#?MODULE.id, Id}) orelse
        error(badarg, [Pid, Id]).

ids() ->
    [Id || #?MODULE{id = Id} <- ets:match_object(?MODULE, r('_', '_', '_'))].


broadcast(Message) ->
    broadcast(ets:first(?MODULE), Message).

broadcast('$end_of_table', _) ->
    ok;
broadcast(Pid, Message) ->
    send(Pid, Message),
    broadcast(ets:next(?MODULE, Pid), Message).


send(Pid, Message) when is_pid(Pid) ->
    outgoing(ets:lookup(?MODULE, Pid), Message);
send(Id, Message) ->
    outgoing(ets:match_object(?MODULE, r('_', Id, '_')), Message).

size() ->
    ets:info(?MODULE, size).

outgoing(Matches, Message) ->
    lists:foreach(
      fun
          (#?MODULE{sender = Sender}) ->
              ok = Sender(Message)
      end,
      Matches).

r(Pid, Id, Sender) ->
    #?MODULE{pid = Pid, id = Id, sender = Sender}.
