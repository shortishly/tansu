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

-module(raft_sm_mnesia_kv).
-export([ckv_delete/3]).
-export([ckv_get/3]).
-export([ckv_set/4]).
-export([ckv_set/5]).
-export([ckv_subscribe/3]).
-export([ckv_test_and_set/5]).
-export([ckv_test_and_set/6]).
-export([ckv_unsubscribe/3]).
-export([expired/1]).
-export([new/0]).

-behaviour(raft_sm).

-include_lib("stdlib/include/qlc.hrl").

-record(?MODULE, {key, parent, expiry, value}).


new() ->
    Attributes = [{attributes, record_info(fields, ?MODULE)},
                  {index, [parent]},
                  {type, ordered_set}],

    Definition = case raft_config:db_schema() of
                     ram ->
                         Attributes;

                     _ ->
                         [{disc_copies, [node()]} | Attributes]
                 end,
    case mnesia:create_table(?MODULE, Definition) of
        {atomic, ok} ->
            {ok, _} = raft_sm_mnesia_expiry:new(),
            {ok, ?MODULE};

        {aborted, {already_exists, _}} ->
            case mnesia:wait_for_tables([?MODULE], raft_config:timeout(mnesia_wait_for_tables)) of
                {timeout, Tables} ->
                    {error, {timeout, Tables}};
                {error, _} = Error ->
                    Error;
                ok ->
                    {ok, _} = raft_sm_mnesia_expiry:new(),
                    {ok, ?MODULE}
            end;

        {aborted, Reason} ->
            {error, Reason}
    end.




ckv_get(Category, Key, ?MODULE = StateMachine) ->
    {do_get(Category, Key), StateMachine}.

ckv_delete(Category, Key, ?MODULE = StateMachine) ->
    {do_delete(Category, Key), StateMachine}.

ckv_set(Category, Key, Value, ?MODULE = StateMachine) ->
    {do_set(Category, Key, Value), StateMachine}.

ckv_set(Category, Key, Value, TTL, ?MODULE = StateMachine) ->
    {do_set(Category, Key, Value, TTL), StateMachine}.

ckv_test_and_set(Category, Key, ExistingValue, NewValue, ?MODULE = StateMachine) ->
    {do_test_and_set(Category, Key, ExistingValue, NewValue), StateMachine}.

ckv_test_and_set(Category, Key, ExistingValue, NewValue, TTL, ?MODULE = StateMachine) ->
    {do_test_and_set(Category, Key, ExistingValue, NewValue, TTL), StateMachine}.

ckv_subscribe(Category, Key, ?MODULE = StateMachine) ->
    {do_subscribe(Category, Key), StateMachine}.

ckv_unsubscribe(Category, Key, ?MODULE = StateMachine) ->
    {do_unsubscribe(Category, Key), StateMachine}.

expired(?MODULE = StateMachine) ->
    {do_expired(), StateMachine}.

    
do_get(Category, Key) ->
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

do_delete(Category, Key) ->
    activity(
      fun
          () ->
              recursive_remove(Category, Key)
      end).

recursive_remove(ParentCategory, ParentKey) ->
    Children = qlc:cursor(
                 qlc:q(
                   [Key || #?MODULE{key = Key, parent = Parent} <- mnesia:table(?MODULE),
                           Parent == {ParentCategory, ParentKey}])),
    cursor_remove(Children, qlc:next_answers(Children)),
    mnesia:delete({?MODULE, {ParentCategory, ParentKey}}).



cursor_remove(Cursor, []) ->
    qlc:delete_cursor(Cursor);
cursor_remove(Cursor, Answers) ->
    lists:foreach(
      fun
          ({Category, Key}) ->
              recursive_remove(Category, Key)
      end,
      Answers),
    cursor_remove(Cursor, qlc:next_answers(Cursor)).

cancel_expiry(Category, Key) ->
    case mnesia:read(?MODULE, {Category, Key}) of
        [#?MODULE{expiry = Previous}] when is_integer(Previous) ->
            raft_sm_mnesia_expiry:cancel(Category, Key, Previous);
        _ ->
            nop
    end.

do_set(Category, Key, Value) ->
    activity(
      fun
          () ->
              cancel_expiry(Category, Key),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    value = Value,
                                    parent = {Category, parent(Key)}})
      end).


do_set(Category, Key, Value, TTL) ->
    activity(
      fun
          () ->
              cancel_expiry(Category, Key),
              Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,
              mnesia:write(#?MODULE{key = {Category, Key},
                                    value = Value,
                                    expiry = Expiry,
                                    parent = {Category, parent(Key)}}),
              raft_sm_mnesia_expiry:set(Category, Key, Expiry)
      end).
    

do_test_and_set(Category, Key, undefined, NewValue) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{}] ->
                      error;

                  [] ->
                      mnesia:write(#?MODULE{key = {Category, Key},
                                            value = NewValue,
                                            parent = {Category, parent(Key)}})
              end
      end);

do_test_and_set(Category, Key, ExistingValue, NewValue) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue,
                            expiry = Previous} = Existing] when is_integer(Previous) ->
                      cancel_expiry(Category, Key),
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    expiry = undefined});

                  [#?MODULE{value = ExistingValue} = Existing] ->
                      mnesia:write(Existing#?MODULE{value = NewValue});

                  [] ->
                      error
              end
      end).


do_test_and_set(Category, Key, undefined, NewValue, TTL) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{}] ->
                      error;

                  [] ->
                      Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,
                      mnesia:write(#?MODULE{key = {Category, Key},
                                            value = NewValue,
                                            expiry = Expiry,
                                            parent = {Category, parent(Key)}}),
                      raft_sm_mnesia_expiry:set(Category, Key, Expiry)
              end
      end);

do_test_and_set(Category, Key, ExistingValue, NewValue, TTL) ->
    activity(
      fun
          () ->
              Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,

              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue,
                            expiry = Previous} = Existing] when is_integer(Previous) ->
                      cancel_expiry(Category, Key),
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    expiry = Expiry}),
                      raft_sm_mnesia_expiry:set(Category, Key, Expiry);


                  [#?MODULE{value = ExistingValue} = Existing] ->
                      mnesia:write(Existing#?MODULE{value = NewValue, expiry = Expiry}),
                      raft_sm_mnesia_expiry:set(Category, Key, Expiry);

                  [] ->
                      error
              end
      end).


do_subscribe(Category, Key) ->
    gproc:reg({p, l, {?MODULE, {Category, Key}}}).

do_unsubscribe(Category, Key) ->
    gproc:unreg({p, l, {?MODULE, {Category, Key}}}).

do_expired() ->
    activity(
      fun
          () ->
              raft_sm_mnesia_expiry:expired()
      end).


parent(Key) when is_atom(Key) ->
    undefined;
parent(Key) ->
    lists:droplast(Key).


activity(F) ->
    mnesia:activity(transaction, F).

