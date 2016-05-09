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

-module(tansu_sm_mnesia_kv).

-export([ckv_delete/3]).
-export([ckv_get/3]).
-export([ckv_set/5]).
-export([ckv_test_and_delete/4]).
-export([ckv_test_and_set/6]).
-export([expired/1]).
-export([install_snapshot/3]).
-export([new/0]).
-export([snapshot/3]).

-behaviour(tansu_sm).

-include_lib("stdlib/include/qlc.hrl").

-record(?MODULE, {key, parent, metadata = #{}, expiry, value}).


new() ->
    case tansu_mnesia:create_table(?MODULE, [{attributes,
                                             record_info(fields, ?MODULE)},
                                            {index, [parent]},
                                            {type, ordered_set}]) of
        ok ->
            case tansu_sm_mnesia_expiry:new() of
                ok ->
                    {ok, ?MODULE};
                Other ->
                    Other
            end;
        Other ->
            Other
    end.

ckv_get(Category, Key, ?MODULE = StateMachine) ->
    {do_get(Category, Key), StateMachine}.

ckv_delete(Category, Key, ?MODULE = StateMachine) ->
    {do_delete(Category, Key), StateMachine}.

ckv_set(Category, Key, Value, Options, ?MODULE = StateMachine) ->
    {do_set(Category, Key, Value, Options), StateMachine}.

ckv_test_and_delete(Category, Key, ExistingValue, ?MODULE = StateMachine) ->
    {do_test_and_delete(Category, Key, ExistingValue), StateMachine}.

ckv_test_and_set(Category, Key, ExistingValue, NewValue, TTL, ?MODULE = StateMachine) ->
    {do_test_and_set(Category, Key, ExistingValue, NewValue, TTL), StateMachine}.

expired(?MODULE = StateMachine) ->
    {do_expired(), StateMachine}.

snapshot(Name, LastApplied, ?MODULE = StateMachine) ->
    {do_snapshot(Name, LastApplied), StateMachine}.

install_snapshot(Name, Data, ?MODULE = StateMachine) ->
    {do_install_snapshot(Name, Data), StateMachine}.

do_get(Category, Key) ->
    activity(
      fun
          () ->
                    case mnesia:read(?MODULE, {Category, Key}) of
                        [#?MODULE{value = Value, metadata = Metadata}] ->
                            {ok, Value, Metadata};

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
    cancel_expiry(ParentCategory, ParentKey),
    case mnesia:read(?MODULE, {ParentCategory, ParentKey}) of
        [#?MODULE{value = Value}] ->
            mnesia:delete({?MODULE, {ParentCategory, ParentKey}}),
            tansu_sm:notify(ParentCategory, ParentKey, #{deleted => Value}),
            ok;

        [] ->
            ok
    end.


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
            tansu_sm_mnesia_expiry:cancel(Category, Key, Previous);
        _ ->
            nop
    end.

do_set(Category, Key, Value, #{parent := Parent, ttl := TTL} = Options) ->
    activity(
      fun
          () ->
              cancel_expiry(Category, Key),
              Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,
              Metadata = maps:without([parent, ttl], Options),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    value = Value,
                                    metadata = Metadata,
                                    expiry = Expiry,
                                    parent = {Category, Parent}}),
              tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
              tansu_sm:notify(Category, Key, #{event => set,
                                               ttl => TTL,
                                               metadata => Metadata,
                                               value => Value}),
              ok
      end);

do_set(Category, Key, Value, #{parent := Parent} = Options) ->
    activity(
      fun
          () ->
              cancel_expiry(Category, Key),
              Metadata = maps:without([parent], Options),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    value = Value,
                                    metadata = Metadata,
                                    parent = {Category, Parent}}),
              tansu_sm:notify(Category, Key, #{event => set,
                                               metadata => Metadata,
                                               value => Value}),
              ok
      end);

do_set(Category, Key, Value, Metadata) ->
    activity(
      fun
          () ->
              cancel_expiry(Category, Key),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    metadata = Metadata,
                                    value = Value}),
              tansu_sm:notify(Category, Key, #{event => set,
                                               metadata => Metadata,
                                               value => Value}),
              ok
      end).



do_test_and_set(Category, Key, undefined, NewValue, #{parent := Parent, ttl := TTL} = Options) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{}] ->
                      error;

                  [] ->
                      Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,
                      Metadata = maps:without([parent, ttl], Options),
                      mnesia:write(#?MODULE{key = {Category, Key},
                                            value = NewValue,
                                            metadata = Metadata,
                                            expiry = Expiry,
                                            parent = {Category, Parent}}),
                      tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       ttl => TTL,
                                                       metadata => Metadata,
                                                       value => NewValue}),
                      ok
              end
      end);


do_test_and_set(Category, Key, undefined, NewValue, #{parent := Parent} = Options) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{}] ->
                      error;

                  [] ->
                      Metadata = maps:without([parent], Options),
                      mnesia:write(#?MODULE{key = {Category, Key},
                                            value = NewValue,
                                            metadata = Metadata,
                                            parent = {Category, Parent}}),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       metadata => Metadata,
                                                       value => NewValue}),
                      ok
              end
      end);

do_test_and_set(Category, Key, undefined, NewValue, Metadata) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{}] ->
                      error;
                  
                  [] ->
                      mnesia:write(#?MODULE{key = {Category, Key},
                                            metadata = Metadata,
                                            value = NewValue}),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       metadata => Metadata,
                                                       value => NewValue}),
                      ok
              end
      end);


do_test_and_set(Category, Key, ExistingValue, NewValue, #{ttl := TTL}) ->
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
                      tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                      ttl => TTL,
                                                      old_value => ExistingValue,
                                                      value => NewValue}),
                      ok;


                  [#?MODULE{value = ExistingValue} = Existing] ->
                      mnesia:write(Existing#?MODULE{value = NewValue, expiry = Expiry}),
                      tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                      ttl => TTL,
                                                      old_value => ExistingValue,
                                                      value => NewValue}),
                      ok;


                  [] ->
                      error
              end
      end);

do_test_and_set(Category, Key, ExistingValue, NewValue, #{}) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue,
                            expiry = Previous} = Existing] when is_integer(Previous) ->
                      cancel_expiry(Category, Key),
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    expiry = undefined}),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       old_value => ExistingValue,
                                                       value => NewValue}),
                      ok;

                  [#?MODULE{value = ExistingValue} = Existing] ->
                      mnesia:write(Existing#?MODULE{value = NewValue}),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       old_value => ExistingValue,
                                                       value => NewValue}),
                      ok;

                  [] ->
                      error
              end
      end).


do_test_and_delete(Category, Key, ExistingValue) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue}] ->
                      recursive_remove(Category, Key);

                  [#?MODULE{}] ->
                      error;
                  [] ->
                      error
              end
      end).

do_snapshot(Name, LastApplied) ->
    activity(
      fun
          () ->
              case {tansu_log:do_first(), tansu_log:do_last()} of
                  {#{index := First}, #{index := Last}} when (First < LastApplied) andalso (Last >= LastApplied) ->
                      #{term := Term} = tansu_log:do_read(LastApplied),
                      tansu_log:do_delete(First, LastApplied),
                      tansu_log:do_write(LastApplied, Term, #{m => tansu_sm, f => apply_snapshot, a => [Name]}),
                      {ok, Checkpoint, _} = mnesia:activate_checkpoint([{min, tables(snapshot)}]),
                      ok = mnesia:backup_checkpoint(Checkpoint, Name),
                      ok = mnesia:deactivate_checkpoint(Checkpoint);
                  
                  _ ->
                      ok
              end
      end).

do_install_snapshot(Name, Data) ->
    ok = filelib:ensure_dir(Name),
    ok = file:write_file(Name, Data),
    {atomic, _} = mnesia:restore(Name, [{recreate_tables, tables(snapshot)}]),
    ok.

tables(snapshot) ->
    [?MODULE, tansu_log, tansu_sm_mnesia_expiry].


do_expired() ->
    activity(
      fun
          () ->
              tansu_sm_mnesia_expiry:expired()
      end).

activity(F) ->
    mnesia:activity(transaction, F).


