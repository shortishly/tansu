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

-module(tansu_ps).
-export([create_table/0]).
-export([term/1]).
-export([term/2]).
-export([id/0]).
-export([increment/2]).
-export([voted_for/1]).
-export([voted_for/2]).

-include("tansu_ps.hrl").

create_table() ->
    Attributes = [{attributes, record_info(fields, ?MODULE)},
                  {type, ordered_set}],

    Definition = case tansu_config:db_schema() of
                     ram ->
                         Attributes;

                     _ ->
                         [{disc_copies, [node()]} | Attributes]
                 end,
    case mnesia:create_table(?MODULE, Definition) of
        {atomic, ok} ->
            new(),
            true;

        {aborted, {already_exists, _}} ->
            true;

        {aborted, Reason} ->
            error(Reason)
    end.

id() ->
    activity(
      fun
          () ->
              mnesia:first(?MODULE)
      end).

term(Id) ->
    activity(
      fun() ->
              case mnesia:read(?MODULE, Id) of
                  [#?MODULE{term = CurrentTerm}] ->
                      CurrentTerm;
                  [] ->
                      error(badarg, Id)
              end
      end).

term(Id, New) ->
    activity(
      fun() ->
              case mnesia:read(?MODULE, Id) of
                  [#?MODULE{term = Current} = PS] when New > Current ->
                      ok = mnesia:write(PS#?MODULE{term = New}),
                      New;

                  [#?MODULE{term = Current}] when New < Current ->
                      error(badarg, Id);

                  [#?MODULE{term = Current}] ->
                      Current;

                  [] ->
                      error(badarg, Id)
              end
      end).

increment(Id, CurrentTerm) ->
    activity(
      fun() ->
              case mnesia:read(?MODULE, Id) of
                  [#?MODULE{term = CurrentTerm} = PS] ->
                      ok = mnesia:write(PS#?MODULE{term = CurrentTerm + 1}),
                      CurrentTerm+1;

                  [#?MODULE{term = _}] ->
                      error(badarg, [Id, CurrentTerm]);

                  [] ->
                      error(badarg, [Id, CurrentTerm])
              end
      end).

voted_for(Id) ->
    activity(
      fun() ->
              case mnesia:read(?MODULE, Id) of
                  [#?MODULE{voted_for = VotedFor}] ->
                      VotedFor;
                  [] ->
                      error(badarg, [Id])
              end
      end).

voted_for(Id, VotedFor) ->
    activity(
      fun() ->
              case mnesia:read(?MODULE, Id) of
                  [#?MODULE{} = PS] ->
                      ok = mnesia:write(
                             PS#?MODULE{voted_for = VotedFor}),
                      VotedFor;
                  [] ->
                      error(badarg, [Id, VotedFor])
              end
      end).


new() ->
    activity(
      fun() ->
              mnesia:write(#?MODULE{id = tansu_uuid:new(), term = 0})
      end).

activity(F) ->
    mnesia:activity(transaction, F).
