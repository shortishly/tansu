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

-module(raft_log).
-export([commit_index/0]).
-export([create_table/0]).
-export([last/0]).

-include("raft_log.hrl").

create_table() ->
    Attributes = [{attributes, record_info(fields, ?MODULE)},
                  {type, ordered_set}],

    Definition = case raft_config:db_schema() of
                     ram ->
                         Attributes;

                     _ ->
                         [{disc_copies, [node()]} | Attributes]
                 end,
    case mnesia:create_table(?MODULE, Definition) of
        {atomic, ok} ->
            true;

        {aborted, {already_exists, _}} ->
            true;

        {aborted, Reason} ->
            error(Reason)
    end.

last() ->
    activity(
      fun
          () ->
              case mnesia:last(?MODULE) of
                  '$end_of_table' ->
                      #{id => 0};

                  Id ->
                      case mnesia:read(?MODULE, Id) of
                          [#?MODULE{id = Id, term = Term, command = Command}] ->
                              #{id => Id, term => Term, command => Command};
                          [] ->
                              error(badarg)
                      end
              end
      end).

commit_index() ->
    #{id := Id} = last(),
    Id.

activity(F) ->
    mnesia:activity(transaction, F).
