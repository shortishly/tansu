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

-module(raft_sm).
-export([get/3]).
-export([new/0]).
-export([set/4]).
-export([tset/5]).

new() ->
    #{}.


get(Category, Key, StateMachine) ->
    case StateMachine of
        #{Category := #{Key := Value}} ->
            Value;

        _ ->
            error(badarg, [Category, Key, StateMachine])
    end.


set(Category, Key, Value, StateMachine) ->
    case StateMachine of
        #{Category := KVS} ->
            StateMachine#{Category := KVS#{Key => Value}};

        _ ->
            set(Category, Key, Value, StateMachine#{Category => #{}})
    end.


tset(Category, Key, undefined, NewValue, StateMachine) ->
    case StateMachine of
        #{Category := #{Key := _}} ->
            error(badarg, [Category, Key, undefined, NewValue, StateMachine]);

        #{Category := KVS} ->
            StateMachine#{Category := KVS#{Key => NewValue}};

        _ ->
            StateMachine#{Category => #{Key => NewValue}}
    end;
tset(Category, Key, ExistingValue, NewValue, StateMachine) ->
    case StateMachine of
        #{Category := #{Key := ExistingValue} = KVS} ->
            StateMachine#{Category := KVS#{Key => NewValue}};

        _ ->
            error(badarg, [Category, Key, undefined, NewValue, StateMachine])
    end.

