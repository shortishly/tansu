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

-export([append_entries/3]).
-export([commit_index/0]).
-export([create_table/0]).
-export([do_clear_table/0]).
-export([do_delete/2]).
-export([do_first/0]).
-export([do_last/0]).
-export([do_read/1]).
-export([do_write/3]).
-export([first/0]).
-export([last/0]).
-export([read/1]).
-export([read/2]).
-export([term_for_index/1]).
-export([trace/1]).
-export([write/2]).

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
            error(badarg, [Reason])
    end.

do_clear_table() ->
    mnesia:clear_table(?MODULE).

first() ->
    activity(fun do_first/0).

last() ->
    activity(fun do_last/0).

do_first() ->
    first_or_last(first).

do_last() ->
    first_or_last(last).

first_or_last(FirstOrLast) ->
    case mnesia:FirstOrLast(?MODULE) of
        '$end_of_table' ->
            #{index => 0, term => 0};
        
        Index ->
            [#?MODULE{
                 index = I,
                 term = T,
                 command = C}] = mnesia:read(?MODULE, Index),
            #{index => I, term => T, command => C}
    end.
    

append_entries(PrevLogIndex, PrevLogTerm, Entries) ->
    activity(
      fun
          () ->
              case {mnesia:last(?MODULE), mnesia:read(?MODULE, PrevLogIndex)} of
                  {'$end_of_table', []} when PrevLogIndex == 0 ->
                      {ok, append_entries(PrevLogIndex, Entries)};

                  {PrevLogIndex, [#?MODULE{term = PrevLogTerm}]} ->
                      {ok, append_entries(PrevLogIndex, Entries)};

                  _ ->
                      {error, unmatched_term}
              end
      end).


append_entries(PrevLogIndex, Entries) ->
    lists:foldl(
      fun
          (#{term := T, command := C}, I) ->
              mnesia:write(#?MODULE{index = I+1,
                                    term = T,
                                    command = C}),
              I+1
      end,
      PrevLogIndex,
      Entries).


read(Index) ->
    activity(
      fun
          () ->
              do_read(Index)
      end).

do_read(Index) ->
    case mnesia:read(?MODULE, Index) of
        [#?MODULE{term = T, command = C}] ->
            #{term => T, command => C};
        [] ->
            error(badarg, [Index])
    end.

read(StartIndex, LastIndex) ->
    activity(
      fun
          () ->
              do_read(StartIndex, LastIndex)
      end).

do_read(StartIndex, LastIndex) ->
    lists:map(fun do_read/1, lists:seq(StartIndex, LastIndex)).


write(Term, Command) ->
    activity(
      fun
          () ->
              Index = commit_index() + 1,
              do_write(Index, Term, Command),
              Index
      end).

do_write(Index, Term, Command) ->
    mnesia:write(#?MODULE{index = Index,
                          term = Term,
                          command = Command}).


commit_index() ->
    #{index := Index} = last(),
    Index.

term_for_index(0) ->
    0;
term_for_index(Index) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, Index) of
                  [#?MODULE{term = Term}] ->
                      Term;
                  [] ->
                      error(badarg, [Index])
              end
      end).


do_delete(First, Last) ->
    lists:foreach(
      fun
          (Index) ->
              mnesia:delete({?MODULE, Index})
      end,
      lists:seq(First, Last)).



activity(F) ->
    mnesia:activity(transaction, F).


trace(true) ->
    recon_trace:calls({?MODULE, '_', '_'},
                      {1000, 500},
                      [{scope, local},
                       {pid, all}]);
trace(false) ->
    recon_trace:clear().
