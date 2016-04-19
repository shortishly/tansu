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
-export([new/0]).
-export([system/3]).
-export([user/3]).


new() ->
    #{user => #{}, system => #{}}.

system(Key, Value, #{system := System} = StateMachine) ->
    StateMachine#{system := System#{Key => Value}}.

user(Key, Value, #{user := User} = StateMachine) ->
    StateMachine#{user := User#{Key => Value}}.

