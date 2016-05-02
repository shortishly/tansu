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

-module(raft_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    common:all(?MODULE).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(any),
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(jsx),
    {ok, _} = application:ensure_all_started(gun),
    cluster_is_available(
      [{availability_tries, 5},
       {availability_timeout, 5000} |
       start_cluster(
         [{cluster_size, 5},
          {cluster_env, raft_uuid:new()} |
          connect_to_docker(
            [{docker_host, "tcp://localhost:2375"},
             {docker_cert_path, undefined},
             {docker_cert, undefined},
             {docker_key, undefined} | Config])])]).


end_per_suite(Config) ->
    disconnect_from_docker(remove_cluster(log_cluster(Config))).

kv_set_test(Config) ->
    ok = kv_set(abc, #{a => 1}, Config),
    until({ok, #{<<"value">> => #{<<"a">> => 1}}}, fun() -> kv_get(abc, Config) end).

kv_get_not_found_test(Config) ->
    not_found = kv_get(def, Config).

kv_delete_not_found_test(Config) ->
    Key = ghi,
    Value = #{b => 2},
    ok = kv_set(Key, Value, Config),
    until({ok, #{<<"value">> => #{<<"b">> => 2}}}, fun() -> kv_get(Key, Config) end),
    ok = kv_delete(Key, Config),
    until(not_found, fun() -> kv_get(Key, Config) end).

until(Pattern, F) ->
    case F() of
        Pattern ->
            ok;

        {error, _} = Error ->
            ct:fail(Error);

        Other ->
            ct:log("~p", [Other]),
            timer:sleep(1000),
            until(Pattern, F)
    end.
    

cluster_is_available(Config) ->
    cluster_is_available(
      config(
        availability_tries, Config),
      config(
        availability_timeout, Config),
      config(
        cluster_size, Config),
      Config).

cluster_is_available(0, _, _, _) ->
    ct:fail(timeout);

cluster_is_available(Tries, Timeout, Size, Config) ->
    case info(Config) of
        {ok, #{<<"consensus">> := #{<<"leader">> := #{<<"connections">> := Connections}} = Consensus}} when map_size(Connections) >= Size - 1 ->
            ct:log("~p", [Consensus]),
            Config;

        {ok, #{<<"consensus">> := #{<<"follower">> := #{<<"leader">> := _, <<"connections">> := Connections}}} = Consensus} when map_size(Connections) >= Size - 1 ->
            ct:log("~p", [Consensus]),
            Config;

        Otherwise ->
            ct:log("~p", [Otherwise]),
            timer:sleep(Timeout),
            cluster_is_available(Tries - 1, Timeout, Size, Config)
    end.


connect_to_docker(Config) ->
    Host = config(docker_host, Config),
    CertPath = config(docker_cert_path, Config),
    Cert = config(docker_cert, Config),
    Key = config(docker_key, Config),
    {ok, Docker} = docker_client:start(Host, CertPath, Cert, Key),
    [{docker, Docker} | Config].

disconnect_from_docker(Config) ->
    docker_client:stop(config(docker, Config)),
    lists:keydelete(docker, 1, Config).

kv_set(Key, Value, Config) ->
    URL = "http://" ++ ip_from_cluster(Config) ++ "/client/keys/" ++ any:to_list(Key),
    Request = {URL, [], "application/json", jsx:encode(Value)},
    request(post, Request).

kv_get(Key, Config) ->
    URL = "http://" ++ ip_from_cluster(Config) ++ "/client/keys/" ++ any:to_list(Key),
    Request = {URL, []},
    request(get, Request).

kv_delete(Key, Config) ->
    URL = "http://" ++ ip_from_cluster(Config) ++ "/client/keys/" ++ any:to_list(Key),
    Request = {URL, []},
    request(delete, Request).


info(Config) ->
    URL = "http://" ++ ip_from_cluster(Config) ++ "/client/info",
    request(get, {URL, []}).

request(Method, Request) ->
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, Headers, Body}} ->
            {ok, decode(Headers, Body)};

        {ok, {{_, 204, _}, _, _}} ->
            ok;

        {ok, {{_, 404, _}, _, _}} ->
            not_found;

        {ok, {{_, Code, Reason}, _, _}} when (Code >= 500) andalso (Code =< 599) ->
            {error, Reason};

        {error, _} = Error ->
            Error
    end.

ip_from_cluster(Config) ->
    Cluster = maps:to_list(config(cluster, Config)),
    {_, IP} = lists:nth(random:uniform(length(Cluster)), Cluster),
    IP.

log_cluster(Config) ->
    log_cluster(config(docker, Config), config(cluster, Config)),
    Config.

log_cluster(Docker, Cluster) ->
    maps:fold(
      fun(Id, _, A) ->
              {ok, Logs} = docker_client:logs_container(Docker, Id),
              ct:log("container: ~s~n~s~n", [binary_to_list(Id), binary_to_list(Logs)]),
              A
      end,
      ignore,
      Cluster).
    

remove_cluster(Config) ->
    remove_cluster(config(docker, Config), config(cluster, Config)),
    lists:keydelete(cluster, 1, Config).

remove_cluster(Docker, Cluster) ->
    maps:fold(
      fun
          (Id, _, _) ->
              docker_client:remove_container(Docker, Id, true, true)
      end,
      ignore,
      Cluster).

start_cluster(Config) ->
    [{cluster, start_cluster(config(docker, Config), config(cluster_env, Config), config(cluster_size, Config))} | Config].

start_cluster(Docker, Env, Size) ->
    lists:foldl(
      fun
          (_, A) ->
              {Id, IP} = start_container(Docker, Env),
              A#{Id => IP}
      end,
      #{},
      lists:seq(1, Size)).

start_container(Docker, Env) ->
    Configuration = #{<<"Image">> => <<"shortishly/raft">>, <<"Env">> => [<<"RAFT_ENVIRONMENT=", Env/bytes>>]},
    {ok, #{<<"Id">> := Id}} = docker_client:create_container(Docker, Configuration),
    ok = docker_client:start_container(Docker, Id),
    {ok, #{<<"NetworkSettings">> := #{<<"Networks">> := #{<<"bridge">> := #{<<"IPAddress">> := IP}}}}} = docker_client:inspect_container(Docker, Id),
    {Id, any:to_list(IP)}.


config(Key, Config) ->
    ?config(Key, Config).


decode(Headers, Body) when is_list(Headers) ->
    decode(maps:from_list(Headers), Body);

decode(#{"content-type" := "application/json"}, JSON) ->
    jsx:decode(JSON, [return_maps]);
decode(#{}, Body) ->
    Body.

    
