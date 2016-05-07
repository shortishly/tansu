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

-module(tansu_sm).

-export([ckv_delete/3]).
-export([ckv_get/3]).
-export([ckv_set/5]).
-export([ckv_test_and_delete/4]).
-export([ckv_test_and_set/6]).
-export([expired/1]).
-export([goodbye/0]).
-export([new/0]).
-export([notify/3]).
-export([subscribe/2]).
-export([unsubscribe/2]).


-type state_machine() :: any().

-callback new() -> {ok, StateMachine :: state_machine()} |
                   {error, Reason :: string()} |
                   {error, Reason :: atom()}.


-callback expired(StateMachine :: state_machine()) -> {[{Category :: atom(), Key :: binary()}], StateMachine :: state_machine()}.


-callback ckv_get(Category :: atom(),
                  Key :: binary(),
                  StateMachine :: state_machine()) -> {{ok, Value :: any()} |
                                                       {error, Reason :: string()} |
                                                       {error, Reason :: atom()},
                                                       StateMachine :: state_machine()}.

-callback ckv_delete(Category :: atom(),
                     Key :: binary(),
                     StateMachine :: state_machine()) -> {ok |
                                                          {error, Reason :: string()} |
                                                          {error, Reason :: atom()},
                                                          StateMachine :: state_machine()}.

-callback ckv_set(Category :: atom(),
                  Key :: binary(),
                  Value :: any(),
                  Options :: map(),
                  StateMachine :: state_machine()) -> {ok |
                                                       {error, Reason :: string()} |
                                                       {error, Reason :: atom()},
                                                       StateMachine :: state_machine()}.


-callback ckv_test_and_delete(Category :: atom(),
                              Key :: binary(),
                              ExistingValue :: any(),
                              StateMachine :: state_machine()) -> {ok |
                                                                   {error, Reason :: string()} |
                                                                   {error, Reason :: atom()},
                                                                   StateMachine :: state_machine()}.
-callback ckv_test_and_set(Category :: atom(),
                           Key :: binary(),
                           ExistingValue :: any(),
                           NewValue :: any(),
                           Options :: map(),
                           StateMachine :: state_machine()) -> {ok |
                                                                {error, Reason :: string()} |
                                                                {error, Reason :: atom()},
                                                                StateMachine :: state_machine()}.

new() ->
    (tansu_config:sm()):new().

ckv_get(Category, Key, StateMachine) ->
    (tansu_config:sm()):ckv_get(Category, Key, StateMachine).

ckv_delete(Category, Key, StateMachine) ->
    (tansu_config:sm()):ckv_delete(Category, Key, StateMachine).

ckv_set(Category, Key, Value, Options, StateMachine) ->
    (tansu_config:sm()):ckv_set(Category, Key, Value, Options, StateMachine).

ckv_test_and_delete(Category, Key, ExistingValue, StateMachine) ->
    (tansu_config:sm()):ckv_test_and_delete(Category, Key, ExistingValue, StateMachine).

ckv_test_and_set(Category, Key, ExistingValue, NewValue, Options, StateMachine) ->
    (tansu_config:sm()):ckv_test_and_set(Category, Key, ExistingValue, NewValue, Options, StateMachine).

expired(StateMachine) ->
    (tansu_config:sm()):expired(StateMachine).


subscribe(Category, Key) ->
    gproc:reg(key(Category, Key)).

unsubscribe(Category, Key) ->
    gproc:unreg(key(Category, Key)).

notify(Category, Key, Data) when map_size(Data) == 1 ->
    [Event] = maps:keys(Data),
    gproc:send(key(Category, Key),
               #{module => ?MODULE,
                 id => erlang:unique_integer(),
                 event => Event,
                 data => Data#{category => Category, key => Key}});

notify(Category, Key, #{id := Id, event := Event} = Data) ->
    gproc:send(key(Category, Key),
               #{module => ?MODULE,
                 id => Id,
                 event => Event,
                 data => maps:without([id, event], Data#{category => Category, key => Key})});

notify(Category, Key, #{event := Event} = Data) ->
    gproc:send(key(Category, Key),
               #{module => ?MODULE,
                 id => erlang:unique_integer(),
                 event => Event,
                 data => maps:without([event], Data#{category => Category, key => Key})}).

key(Category, Key) ->
    {p, l, {?MODULE, {Category, Key}}}.
    
goodbye() ->
    gproc:goodbye().
