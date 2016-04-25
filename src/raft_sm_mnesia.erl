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

-module(raft_sm_mnesia).
-export([ckv_get/3]).
-export([ckv_set/4]).
-export([ckv_test_and_set/5]).
-export([new/0]).

-behaviour(raft_sm).


-record(?MODULE, {key, parent, value}).


new() ->
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
            {ok, ?MODULE};

        {aborted, {already_exists, _}} ->
            {atomic, ok} = mnesia:clear_table(?MODULE),
            {ok, ?MODULE};

        {aborted, Reason} ->
            {error, Reason}
    end.


ckv_get(Category, Key, ?MODULE = StateMachine) ->
    {ckv_get(Category, Key), StateMachine}.

ckv_set(Category, Key, Value, ?MODULE = StateMachine) ->
    {ckv_set(Category, Key, Value), StateMachine}.

ckv_test_and_set(Category, Key, undefined, NewValue, ?MODULE = StateMachine) ->
    {ckv_test_and_set(Category, Key, undefined, NewValue), StateMachine}.


    
ckv_get(Category, Key) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = Value}] ->
                      {ok, Value};

                  [] ->
                      {error, not_found}
              end
      end).

ckv_set(Category, Key, Value) ->
    activity(
      fun
          () ->
              mnesia:write(#?MODULE{key = {Category, Key}, value = Value})
      end).

ckv_test_and_set(Category, Key, undefined, NewValue) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{}] ->
                      error;

                  [] ->
                      mnesia:write(#?MODULE{key = {Category, Key}, value = NewValue})
              end
      end);

ckv_test_and_set(Category, Key, ExistingValue, NewValue) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue}] ->
                      mnesia:write(#?MODULE{key = {Category, Key}, value = NewValue});

                  [] ->
                      error
              end
      end).

activity(F) ->
    mnesia:activity(transaction, F).
