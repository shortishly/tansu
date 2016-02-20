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

-module(raft_timeout).
-export([election/0]).
-export([leader/0]).


election() ->
    raft_random:uniform_random_range(raft_config:election_low_timeout(),
                                     raft_config:election_high_timeout()).

leader() ->
    raft_random:uniform_random_range(raft_config:leader_low_timeout(),
                                     raft_config:leader_high_timeout()).
