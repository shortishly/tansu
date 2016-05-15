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
-export([ckv_get_children_of/3]).
-export([ckv_set/5]).
-export([ckv_test_and_delete/4]).
-export([ckv_test_and_set/6]).
-export([expired/1]).
-export([install_snapshot/3]).
-export([new/0]).
-export([snapshot/3]).

-behaviour(tansu_sm).

-include_lib("stdlib/include/qlc.hrl").
-include("tansu_log.hrl").

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

ckv_get_children_of(Category, Key, ?MODULE = StateMachine) ->
    {do_get_children_of(Category, Key), StateMachine}.

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

do_get_children_of(ParentCategory, ParentKey) ->
    activity(
      fun
          () ->
              qlc:fold(
                fun
                    (#?MODULE{key = Key, value = Value, metadata = Metadata}, A) ->
                        A#{Key => {Value, Metadata}}
                end,
                #{},
                qlc:q(
                  [Child || #?MODULE{parent = Parent} = Child <- mnesia:table(?MODULE),
                            Parent == {ParentCategory, ParentKey}]))
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
        [#?MODULE{value = Value, metadata = Metadata}] ->
            mnesia:delete({?MODULE, {ParentCategory, ParentKey}}),
            tansu_sm:notify(ParentCategory, ParentKey, #{event => delete, value => Value, metadata => Metadata}),
            {ok, Value};

        [] ->
            {ok, undefined}
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
        [#?MODULE{expiry = Previous, value = Current}] when is_integer(Previous) ->
            tansu_sm_mnesia_expiry:cancel(Category, Key, Previous),
            Current;

        [#?MODULE{value = Current}] ->
            Current;

        _ ->
            undefined
    end.

do_set(Category, Key, Value, #{parent := Parent, ttl := TTL} = Options) ->
    activity(
      fun
          () ->
              PreviousValue = cancel_expiry(Category, Key),
              Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,
              Metadata = maps:without([parent, ttl], Options),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    value = Value,
                                    metadata = Metadata,
                                    expiry = Expiry,
                                    parent = {Category, Parent}}),
              tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
              case PreviousValue of
                  undefined ->
                      tansu_sm:notify(Category, Key, #{event => create,
                                                       ttl => TTL,
                                                       metadata => Metadata,
                                                       value => Value});

                  _ ->
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       ttl => TTL,
                                                       metadata => Metadata,
                                                       previous => PreviousValue,
                                                       value => Value})
              end,
              {ok, PreviousValue}
      end);

do_set(Category, Key, Value, #{parent := Parent} = Options) ->
    activity(
      fun
          () ->
              PreviousValue = cancel_expiry(Category, Key),
              Metadata = maps:without([parent], Options),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    value = Value,
                                    metadata = Metadata,
                                    parent = {Category, Parent}}),
              case PreviousValue of
                  undefined ->
                      tansu_sm:notify(Category, Key, #{event => create,
                                                       metadata => Metadata,
                                                       value => Value});

                  _ ->
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       metadata => Metadata,
                                                       previous => PreviousValue,
                                                       value => Value})
              end,
              {ok, PreviousValue}
      end);

do_set(Category, Key, Value, Metadata) ->
    activity(
      fun
          () ->
              PreviousValue = cancel_expiry(Category, Key),
              mnesia:write(#?MODULE{key = {Category, Key},
                                    metadata = Metadata,
                                    value = Value}),
              case PreviousValue of
                  undefined ->
                      tansu_sm:notify(Category, Key, #{event => create,
                                                       metadata => Metadata,
                                                       value => Value});
                  _ ->
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       metadata => Metadata,
                                                       previous => PreviousValue,
                                                       value => Value})
              end,
              {ok, PreviousValue}
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
                      tansu_sm:notify(Category, Key, #{event => create,
                                                       ttl => TTL,
                                                       metadata => Metadata,
                                                       value => NewValue}),
                      {ok, undefined}
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
                      tansu_sm:notify(Category, Key, #{event => create,
                                                       metadata => Metadata,
                                                       value => NewValue}),
                      {ok, undefined}
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
                      tansu_sm:notify(Category, Key, #{event => create,
                                                       metadata => Metadata,
                                                       value => NewValue}),
                      {ok, undefined}
              end
      end);


do_test_and_set(Category, Key, ExistingValue, NewValue, #{ttl := TTL} = Options) ->
    activity(
      fun
          () ->
              Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,

              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue,
                            expiry = Previous} = Existing] when is_integer(Previous) ->
                      cancel_expiry(Category, Key),
                      Metadata = maps:without([ttl], Options),
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    metadata = Metadata,
                                                    expiry = Expiry}),
                      tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       ttl => TTL,
                                                       metadata => Metadata,
                                                       previous => ExistingValue,
                                                       value => NewValue}),
                      {ok, ExistingValue};


                  [#?MODULE{value = ExistingValue} = Existing] ->
                      Metadata = maps:without([ttl], Options),
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    expiry = Expiry,
                                                    metadata = Metadata}),
                      tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       ttl => TTL,
                                                       metadata => Metadata,
                                                       previous => ExistingValue,
                                                       value => NewValue}),
                      {ok, ExistingValue};


                  [] ->
                      error
              end
      end);

do_test_and_set(Category, Key, ExistingValue, NewValue, Metadata) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{value = ExistingValue,
                            expiry = Previous} = Existing] when is_integer(Previous) ->
                      cancel_expiry(Category, Key),
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    metadata = Metadata,
                                                    expiry = undefined}),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       metadata => Metadata,
                                                       previous => ExistingValue,
                                                       value => NewValue}),
                      {ok, ExistingValue};

                  [#?MODULE{value = ExistingValue} = Existing] ->
                      mnesia:write(Existing#?MODULE{value = NewValue,
                                                    metadata = Metadata}),
                      tansu_sm:notify(Category, Key, #{event => set,
                                                       metadata => Metadata,
                                                       previous => ExistingValue,
                                                       value => NewValue}),
                      {ok, ExistingValue};

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

temporary() ->
    Unique = erlang:phash2({erlang:phash2(node()),
                            erlang:monotonic_time(),
                            erlang:unique_integer()}),
    filename:join(
      tansu_config:directory(snapshot),
      "snapshot-" ++ integer_to_list(Unique)).


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

                      Temporary = temporary(),
                      ok = filelib:ensure_dir(Temporary),
                      ok = mnesia:backup_checkpoint(Checkpoint, Temporary),
                      ok = mnesia:deactivate_checkpoint(Checkpoint),

                      Filename = filename:join(tansu_config:directory(snapshot), Name),
                      {ok, _} = mnesia:traverse_backup(
                                  Temporary,
                                  Filename,
                                  fun
                                      (#tansu_log{index = Index}, A) when Index == LastApplied ->
                                          {[#tansu_log{index = LastApplied,
                                                       term = Term,
                                                       command = #{m => tansu_sm,
                                                                   f => apply_snapshot,
                                                                   a => [Name]}}], A};

                                      (#tansu_log{}, A) ->
                                          %% Drop all log records from the snapshot.
                                          {[], A};

                                      (Other, A) ->
                                          {[Other], A}
                                  end,
                                  unused),
                      ok = file:delete(Temporary);

                  _ ->
                      ok
              end
      end).

do_install_snapshot(Name, Data) ->
    Temporary = temporary(),
    ok = filelib:ensure_dir(Temporary),
    ok = file:write_file(Temporary, Data),
    Transformed = filename:join(tansu_config:directory(snapshot), Name),
    {ok, _} = mnesia:traverse_backup(
                Temporary,
                Transformed,
                fun
                    ({schema, db_nodes, _}, A) ->
                        %% Ensure that the schema DB nodes is just
                        %% this one.
                        {[{schema, db_nodes, [node()]}], A};

                    ({schema, Tab, Options}, A) ->
                        {[{schema,
                           Tab,
                           lists:map(
                             fun
                                 ({Key, Nodes}) when (length(Nodes) > 0) andalso
                                                     (Key == ram_copies orelse
                                                      Key == disc_copies orelse
                                                      Key == disc_only_copies) ->
                                     {Key, [node()]};

                                 ({Key, Value}) ->
                                     {Key, Value}
                             end,
                             Options)}],
                         A};

                    (Other, A) ->
                        {[Other], A}
                end,
                unused),
    ok = file:delete(Temporary),
    {atomic, _} = mnesia:restore(Transformed, [{recreate_tables, tables(snapshot)}]),
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


