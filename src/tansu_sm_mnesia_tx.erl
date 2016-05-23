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

-module(tansu_sm_mnesia_tx).
-export([current/0]).
-export([new/0]).
-export([next/0]).
-record(?MODULE, {id, tx}).

new() ->
    Attributes = [{attributes, record_info(fields, ?MODULE)},
                  {type, ordered_set}],

    Definition = case tansu_config:db_schema() of
                     ram ->
                         Attributes;

                     _ ->
                         [{disc_copies, [node()]} | Attributes]
                 end,
    case mnesia:create_table(?MODULE, Definition) of
        {atomic, ok} ->
            tansu_mnesia:activity(
              fun() ->
                      mnesia:write(#?MODULE{id = tansu_uuid:new(), tx = 0})
              end),
            ok;

        {aborted, {already_exists, _}} ->
            ok;

        {aborted, Reason} ->
            {error, Reason}
    end.

current() ->
    tansu_mnesia:activity(
      fun
          () ->
              [#?MODULE{tx = Tx}] = mnesia:read(
                                      ?MODULE,
                                      mnesia:first(?MODULE)),
              Tx
      end).

next() ->
    [#?MODULE{tx = Tx} = TxId] = mnesia:read(
                                   ?MODULE,
                                   mnesia:first(?MODULE)),
    ok = mnesia:write(
           TxId#?MODULE{tx = Tx + 1}),
    Tx.



