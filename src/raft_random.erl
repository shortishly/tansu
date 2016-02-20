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

-module(raft_random).
-export([seed/0]).
-export([uniform_random_range/2]).


seed() ->
    random:seed(erlang:phash2(node()),
                erlang:monotonic_time(),
                erlang:unique_integer()).


uniform_random_range(Low, High) ->
    Low + random:uniform(High-Low).
