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
-export([broadcast/1]).
-export([close/1]).
-export([delete/1]).
-export([new/3]).
-export([send/2]).
-export([size/0]).

-on_load(on_load/0).

-record(?MODULE, {
           pid :: pid(),
           sender :: fun(),
           closer :: fun()
          }).

on_load() ->
    crown_table:reuse(?MODULE, ordered_set).


new(Pid, Sender, Closer) ->
    ets:insert_new(?MODULE, r(Pid, Sender, Closer)) orelse
        error(badarg, [Pid, Sender, Closer]).

delete(Pid) ->
    ets:delete(?MODULE, Pid).

close(Pid) ->
    [#?MODULE{closer = Closer}] = ets:lookup(?MODULE, Pid),
    Closer().

broadcast(Message) ->
    broadcast(ets:first(?MODULE), Message).

broadcast('$end_of_table', _) ->
    ok;
broadcast(Pid, Message) ->
    send(Pid, Message),
    broadcast(ets:next(?MODULE, Pid), Message).


send(Pid, Message) when is_pid(Pid) ->
    outgoing(ets:lookup(?MODULE, Pid), Message);
send(Id, Message) when is_binary(Id) ->
    send(raft_connection_association:pid_for(Id), Message).

size() ->
    ets:info(?MODULE, size).

outgoing(Matches, Message) ->
    lists:foreach(
      fun
          (#?MODULE{sender = Sender}) ->
              ok = Sender(Message)
      end,
      Matches).

r(Pid, Sender, Closer) ->
    #?MODULE{pid = Pid, sender = Sender, closer = Closer}.
