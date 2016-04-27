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

-module(raft_sm_maps).
-export([ckv_delete/3]).
-export([ckv_get/3]).
-export([new/0]).
-export([ckv_set/4]).
-export([ckv_test_and_set/5]).

-behaviour(raft_sm).

new() ->
    {ok, #{}}.


ckv_get(Category, Key, StateMachine) ->
    case StateMachine of
        #{Category := #{Key := Value}} ->
            {{ok, Value}, StateMachine};

        _ ->
            {{error, not_found}, StateMachine}
    end.


ckv_delete(Category, Key, StateMachine) ->
    case StateMachine of
        #{Category := KVS} ->
            {ok, StateMachine#{Category := maps:without([Key], KVS)}};
        _ ->
            {ok, StateMachine}
    end.


ckv_set(Category, Key, Value, StateMachine) ->
    case StateMachine of
        #{Category := KVS} ->
            {ok, StateMachine#{Category := KVS#{Key => Value}}};

        _ ->
            ckv_set(Category, Key, Value, StateMachine#{Category => #{}})
    end.


ckv_test_and_set(Category, Key, undefined, NewValue, StateMachine) ->
    case StateMachine of
        #{Category := #{Key := _}} ->
            {error, StateMachine};

        #{Category := KVS} ->
            {ok, StateMachine#{Category := KVS#{Key => NewValue}}};
        
        _ ->
            {ok, StateMachine#{Category => #{Key => NewValue}}}
    end;
ckv_test_and_set(Category, Key, ExistingValue, NewValue, StateMachine) ->
    case StateMachine of
        #{Category := #{Key := ExistingValue} = KVS} ->
            {ok, StateMachine#{Category := KVS#{Key => NewValue}}};

        _ ->
            {error, StateMachine}
    end.

