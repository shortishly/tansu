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

-module(tansu_mnesia).
-export([activity/1]).
-export([create_table/2]).

-spec create_table(Table :: atom(), Options :: list()) -> ok | {error, Reason :: atom()} | {timeout, Tables :: list(atom())}.

create_table(Table, Options) ->
    Definition = case tansu_config:db_schema() of
                     ram ->
                         Options;

                     _ ->
                         [{disc_copies, [node()]} | Options]
                 end,
    case mnesia:create_table(Table, Definition) of
        {atomic, ok} ->
            ok;

        {aborted, {already_exists, _}} ->
            case mnesia:wait_for_tables([Table], tansu_config:timeout(mnesia_wait_for_tables)) of
                {timeout, Tables} ->
                    {timeout, Tables};

                {error, _} = Error ->
                    Error;

                ok ->
                    ok
            end;

        {aborted, Reason} ->
            {error, Reason}
    end.

activity(F) ->
    mnesia:activity(transaction, F).
