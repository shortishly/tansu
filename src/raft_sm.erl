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

-type state_machine() :: any().

-callback new() -> {ok, StateMachine :: state_machine()} |
                   {error, Reason :: string()} |
                   {error, Reason :: atom()}.

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
                  StateMachine :: state_machine()) -> {ok |
                                                       {error, Reason :: string()} |
                                                       {error, Reason :: atom()},
                                                       StateMachine :: state_machine()}.

-callback ckv_test_and_set(Category :: atom(),
                           Key :: binary(),
                           ExistingValue :: any(),
                           NewValue :: any(),
                           StateMachine :: state_machine()) -> {ok |
                                                                {error, Reason :: string()} |
                                                                {error, Reason :: atom()},
                                                                StateMachine :: state_machine()}.

