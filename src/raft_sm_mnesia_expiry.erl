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

-module(raft_sm_mnesia_expiry).
-export([cancel/3]).
-export([expired/0]).
-export([new/0]).
-export([set/3]).

-record(?MODULE, {composite, category, key}).

new() ->
    Attributes = [{attributes, record_info(fields, ?MODULE)},
                  {type, ordered_set}],

    Definition = case raft_config:db_schema() of
                     ram ->
                         Attributes;

                     _ ->
                         [{disc_copies, [node()]} | Attributes]
                 end,
    case mnesia:create_table(?MODULE, Definition) of
        {atomic, ok} ->
            {ok, ?MODULE};

        {aborted, {already_exists, _}} ->
            case mnesia:wait_for_tables([?MODULE], raft_config:timeout(mnesia_wait_for_tables)) of
                {timeout, Tables} ->
                    {error, {timeout, Tables}};
                {error, _} = Error ->
                    Error;
                ok ->
                    {ok, ?MODULE}
            end;

        {aborted, Reason} ->
            {error, Reason}
    end.

cancel(Category, Key, Expiry) ->
    mnesia:delete_object(#?MODULE{composite = {Expiry, Category, Key},
                                  category = Category,
                                  key = Key}).

set(Category, Key, Expiry) ->
    mnesia:write(#?MODULE{composite = {Expiry, Category, Key},
                          category = Category,
                          key = Key}).

expired() ->
    expired(calendar:datetime_to_gregorian_seconds(erlang:universaltime())).

expired(RightNow) ->
    expired(RightNow, mnesia:first(?MODULE), []).

expired(RightNow, {Expiry, Category, Key} = Composite, A) when RightNow > Expiry ->
    expired(RightNow, mnesia:next(?MODULE, Composite), [{Category, Key} | A]);
expired(_, _, A) ->
    A.



        
            
    