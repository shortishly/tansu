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

-module(raft_app).
-behaviour(application).

-include("raft_log.hrl").
-include("raft_ps.hrl").

-export([create_schema/0]).
-export([create_tables/0]).
-export([endpoints/1]).
-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    try
        create_schema() andalso create_tables(),
        {ok, Sup} = raft_sup:start_link(),
        start_advertiser(raft_tcp_advertiser),
        [raft:trace(true) || raft_config:enabled(debug)],
        {ok, Sup, #{listeners => [start_http(http)]}}
    catch
        _:Reason ->
            {error, Reason}
    end.


stop(#{listeners := Listeners}) ->
    lists:foreach(fun cowboy:stop_listener/1, Listeners);
stop(_State) ->
    ok.


start_advertiser(Advertiser) ->
    [mdns_discover_sup:start_child(Advertiser) || raft_config:can(discover)],
    [mdns_advertise_sup:start_child(Advertiser) || raft_config:can(advertise)].

start_http(Prefix) ->
    {ok, _} = cowboy:start_http(
                Prefix,
                raft_config:acceptors(Prefix),
                [{port, raft_config:port(Prefix)}],
                [{env, [dispatch(Prefix)]}]),
    Prefix.


dispatch(Prefix) ->
    {dispatch, cowboy_router:compile(resources(Prefix))}.


resources(http) ->
    [{'_', endpoints(server) ++ endpoints(client)}].


endpoints(server) ->
    [endpoint(server, raft_api_server_resource)];
endpoints(client) ->
    [endpoint(client, "/keys/[...]", raft_api_client_keys_resource),
     endpoint(client, "/info", raft_api_client_info_resource),
     endpoint(client, "/version", raft_api_client_version_resource)].


endpoint(Endpoint, Module) ->
    endpoint(Endpoint, undefined, Module, []).

endpoint(Endpoint, Pattern, Module) ->
    endpoint(Endpoint, Pattern, Module, []).

endpoint(Endpoint, undefined, Module, Parameters) ->
    {raft_config:endpoint(Endpoint), Module, Parameters};

endpoint(Endpoint, Pattern, Module, Parameters) ->
    {raft_config:endpoint(Endpoint) ++ Pattern, Module, Parameters}.
    


create_schema() ->
    case raft_config:db_schema() of
        ram ->
            case mnesia:table_info(schema, ram_copies) of
                [] ->
                    false;
                [_] ->
                    true
            end;

        _ ->
            case mnesia:table_info(schema, disc_copies) of
                [] ->
                    ok = application:stop(mnesia),

                    case mnesia:create_schema([node()]) of
                        {error, {_, {already_exists, _}}} ->
                            ok = application:start(mnesia),
                            true;

                        ok ->
                            ok = application:start(mnesia),
                            true;

                        {error, Reason} ->
                            error(Reason)
                    end;

                [_] ->
                    true
            end
    end.

create_tables() ->
    raft_ps:create_table() andalso raft_log:create_table().
