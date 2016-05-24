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
-export([current/2]).
-export([expired/1]).
-export([install_snapshot/3]).
-export([new/0]).
-export([snapshot/3]).

-behaviour(tansu_sm).

-include_lib("stdlib/include/qlc.hrl").
-include("tansu_log.hrl").

-record(?MODULE, {key, parent, metadata = #{}, expiry, value, created, updated, content_type}).


new() ->
    {lists:foldl(
       fun
           (Action, ok) ->
               Action();
           
           (_, A) ->
               A
       end,
       ok,
       [fun init/0, fun tansu_sm_mnesia_expiry:new/0, fun tansu_sm_mnesia_tx:new/0]), ?MODULE}.

init() ->
    tansu_mnesia:create_table(
      ?MODULE,
      [{attributes,
        record_info(fields, ?MODULE)},
       {index, [parent]},
       {type, ordered_set}]).
    

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

current(Metric, StateMachine) ->
    {do_current(Metric), StateMachine}.
    

do_get(Category, Key) ->
    activity(
      fun
          () ->
              case {mnesia:read(?MODULE, {Category, Key}), calendar:datetime_to_gregorian_seconds(erlang:universaltime())} of
                  {[#?MODULE{value = Value, expiry = undefined, metadata = Metadata} = Record], _} ->
                      {ok, Value, Metadata#{tansu => metadata_from_record(Record)}};
                  
                  {[#?MODULE{value = Value, expiry = Expiry, metadata = Metadata} = Record], Now} when Expiry >= Now ->
                      {ok, Value, Metadata#{tansu => metadata_from_record(Record)}};

                  {[#?MODULE{expiry = Expiry}], Now} when Expiry < Now ->
                      {error, not_found};
                  
                  {[], _} ->
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
        [#?MODULE{value = Value, metadata = Metadata} = Record] ->
            mnesia:delete({?MODULE, {ParentCategory, ParentKey}}),
            TxId = tansu_sm_mnesia_tx:next(),
            tansu_sm:notify(
              ParentCategory,
              ParentKey,
              #{event => delete,
                id => TxId,
                value => Value,
                metadata => Metadata#{tansu => metadata_from_record(Record#?MODULE{updated = TxId})}}),
            {ok, Value, Metadata#{tansu => #{current => metadata_from_record(Record#?MODULE{updated = TxId}),
                                             previous => metadata_from_record(Record)}}};

        [] ->
            {ok, undefined, #{}}
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
        [#?MODULE{expiry = Previous} = Current] when is_integer(Previous) ->
            tansu_sm_mnesia_expiry:cancel(Category, Key, Previous),
            Current;

        [#?MODULE{} = Current]->
            Current;

        _ ->
            undefined
    end.

do_set(Category, Key, Value, Metadata) ->
    activity(
      fun
          () ->
              case cancel_expiry(Category, Key) of
                  undefined ->
                      write_tx(
                        Category,
                        create,
                        #{key => Key,
                          value => Value},
                        Metadata,
                        undefined);

                  #?MODULE{created = Created} = PreviousValue ->
                      write_tx(
                        Category,
                        set,
                        #{key => Key,
                          value => Value,
                          created => Created},
                        Metadata,
                        PreviousValue)
              end
      end).


do_test_and_set(Category, Key, undefined, NewValue, #{tansu := #{updated := Index}} = Metadata) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{created = Created,
                            updated = Index} = ExistingValue] ->
                      cancel_expiry(Category, Key),
                      write_tx(
                        Category,
                        set,
                        #{key => Key,
                          created => Created,
                          value => NewValue},
                        Metadata,
                        ExistingValue,
                        #{type => cas});

                  [#?MODULE{}] ->
                      %% Not the value that we were expecting.
                      error;

                  [] ->
                      error
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
                      write_tx(
                        Category,
                        set,
                        #{key => Key,
                          value => NewValue},
                        Metadata,
                        undefined,
                        #{type => cas})
              end
      end);

do_test_and_set(Category, Key, ExistingValue, NewValue, Metadata) ->
    activity(
      fun
          () ->
              case mnesia:read(?MODULE, {Category, Key}) of
                  [#?MODULE{created = Created,
                            value = ExistingValue} = Previous] ->
                      cancel_expiry(Category, Key),
                      write_tx(
                        Category,
                        set,
                        #{key => Key,
                          created => Created,
                          value => NewValue},
                        Metadata,
                        Previous,
                        #{type => cas});

                  [#?MODULE{}] ->
                      %% Not the value that we were expecting.
                      error;

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


write_tx(Category, Event, KV, Metadata, PreviousValue) ->
    write_tx(Category, Event, KV, Metadata, PreviousValue, #{}).

write_tx(Category, Event, #{value := NewValue} = KV, Metadata, #?MODULE{value = PreviousValue, content_type = <<"application/json">>} = ExistingValue, Commentary) ->
    Record = integrate_metadata(to_record(Category, KV), Metadata),
    mnesia:write(Record),
    notification(Event, Record, PreviousValue, Commentary),
    {ok, NewValue, Metadata#{tansu => #{previous => metadata_from_record(ExistingValue, #{value => jsx:decode(PreviousValue)}),
                                        current => metadata_from_record(Record)}}};

write_tx(Category, Event, #{value := NewValue} = KV, Metadata, #?MODULE{value = PreviousValue} = ExistingValue, Commentary) ->
    Record = integrate_metadata(to_record(Category, KV), Metadata),
    mnesia:write(Record),
    notification(Event, Record, PreviousValue, Commentary),
    {ok, NewValue, Metadata#{tansu => #{previous => metadata_from_record(ExistingValue, #{value => PreviousValue}),
                                        current => metadata_from_record(Record)}}};

write_tx(Category, Event, #{value := NewValue} = KV, Metadata, undefined = PreviousValue, Commentary) ->
    Record = integrate_metadata(to_record(Category, KV), Metadata),
    mnesia:write(Record),
    notification(Event, Record, PreviousValue, Commentary),
    {ok, NewValue, Metadata#{tansu => #{current => metadata_from_record(Record)}}}.

integrate_metadata(#?MODULE{key = {Category, Key}} = KV, Metadata) ->
    maps:fold(
      fun
          (parent, Parent, A) ->
              A#?MODULE{parent = {Category, Parent}};

          (ttl, TTL, A) ->
              Expiry = calendar:datetime_to_gregorian_seconds(erlang:universaltime()) + TTL,
              tansu_sm_mnesia_expiry:set(Category, Key, Expiry),
              A#?MODULE{expiry = Expiry};

          (content_type, ContentType, A) ->
              A#?MODULE{content_type = ContentType};

          (_, _, A) ->
              A
      end,
      KV,
      maps:get(tansu, Metadata, #{})).

to_record(Category, #{created := _, updated := _} = M) ->
    maps:fold(
      fun
          (key, Key, A) ->
              A#?MODULE{key = {Category, Key}};

          (value, Value, A) ->
              A#?MODULE{value = Value};

          (metadata, Metadata, A) ->
              A#?MODULE{metadata = maps:without([tansu], Metadata)};

          (created, Created, A) ->
              A#?MODULE{created = Created};

          (updated, Updated, A) ->
              A#?MODULE{updated = Updated};

          (content_type, ContentType, A) ->
              A#?MODULE{content_type = ContentType}
      end,
      #?MODULE{},
      M);
to_record(Category, #{created := _} = M) ->
    to_record(Category, M#{updated => tansu_sm_mnesia_tx:next()});
to_record(Category, M) ->
    TxId = tansu_sm_mnesia_tx:next(),
    to_record(Category, M#{created => TxId, updated => TxId}).
    
notification(Event, #?MODULE{key = {Category, Key}, updated = Updated, value = Value, metadata = Metadata} = Record, undefined, Commentary) ->
    tansu_sm:notify(
      Category,
      Key,
      #{event => Event,
        id => Updated,
        metadata => Metadata#{tansu => metadata_from_record(Record, Commentary)},
        value => Value});

notification(Event, #?MODULE{key = {Category, Key}, updated = Updated, value = Value, metadata = Metadata} = Record, Previous, Commentary) ->
    tansu_sm:notify(
      Category,
      Key,
      #{event => Event,
        id => Updated,
        metadata => Metadata#{tansu => metadata_from_record(Record, Commentary)},
        previous => Previous,
        value => Value}).

metadata_from_record(#?MODULE{} = Record) ->
    metadata_from_record(Record, #{}).

metadata_from_record(#?MODULE{} = Record, A) ->
    metadata_from_record(
      content_type,
      Record#?MODULE.content_type,
      metadata_from_record(
        created,
        Record#?MODULE.created,
        metadata_from_record(
          updated,
          Record#?MODULE.updated,
          metadata_from_record(
            parent,
            Record#?MODULE.parent,
            metadata_from_record(
              expiry,
              Record#?MODULE.expiry,
              A))))).

metadata_from_record(_, undefined, A) ->
    A;
metadata_from_record(parent, {_Category, Parent}, A) ->
    A#{parent => Parent};
metadata_from_record(expiry, Expiry, A) ->
    case calendar:datetime_to_gregorian_seconds(erlang:universaltime()) of
        Now when (Expiry - Now) > 0 ->
            A#{ttl => Expiry - Now};
        _ ->
            A#{ttl => 0}
        end;
metadata_from_record(Key, Value, A) ->
    A#{Key => Value}.

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
    [?MODULE, tansu_log, tansu_sm_mnesia_expiry, tansu_sm_mnesia_tx].


do_expired() ->
    activity(
      fun
          () ->
              tansu_sm_mnesia_expiry:expired()
      end).

activity(F) ->
    mnesia:activity(transaction, F).

do_current(index) ->
    tansu_sm_mnesia_tx:current().
