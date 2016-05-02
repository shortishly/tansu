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

-module(docker_client).
-behaviour(gen_server).

-export([code_change/3]).
-export([create_container/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([info/1]).
-export([init/1]).
-export([inspect_container/2]).
-export([logs_container/2]).
-export([kill_container/2]).
-export([remove_container/4]).
-export([start/4]).
-export([start_container/2]).
-export([stop/1]).
-export([terminate/2]).

-include_lib("kernel/include/inet.hrl").
-include_lib("public_key/include/public_key.hrl").


start(Host, CertPath, Cert, Key) ->
    gen_server:start(?MODULE, [Host, CertPath, Cert, Key], []).

info(Docker) ->
    gen_server:call(Docker, info, infinity).

create_container(Docker, Detail) ->
    gen_server:call(Docker, {create_container, Detail}, infinity).

start_container(Docker, ContainerId) ->
    gen_server:call(Docker, {start_container, ContainerId}, infinity).

kill_container(Docker, ContainerId) ->
    gen_server:call(Docker, {kill_container, ContainerId}, infinity).

inspect_container(Docker, ContainerId) ->
    gen_server:call(Docker, {inspect_container, ContainerId}, infinity).

remove_container(Docker, ContainerId, Volumes, Force) ->
    gen_server:call(Docker, {remove_container, ContainerId, Volumes, Force}, infinity).

logs_container(Docker, ContainerId) ->
    gen_server:call(Docker, {logs_container, ContainerId}, infinity).
    

stop(Docker) ->
    gen_server:cast(Docker, stop).


init([InHost, InCertPath, InCert, InKey]) ->
    case connection(InHost, InCertPath, InCert, InKey) of
        {ok, #{host := Host,
               port := Port,
               cert := Cert,
               key := Key}} ->

            case gun:open(Host,
                          Port,
                          #{transport => ssl,
                            transport_opts => haystack_docker_util:ssl(Cert,
                                                                       Key)}) of
                {ok, Pid} ->
                    {ok, #{docker => Pid,
                           monitor => monitor(process, Pid),
                           requests => #{},
                           host => Host,
                           port => Port,
                           cert => Cert,
                           key => Key}};

                {error, Reason} ->
                    {stop, Reason}
            end;

        {ok, #{host := Host,
               port := Port}} ->

            case gun:open(Host,
                          Port,
                          #{transport => tcp}) of
                {ok, Pid} ->
                    {ok, #{docker => Pid,
                           monitor => monitor(process, Pid),
                           requests => #{},
                           host => Host,
                           port => Port}};

                {error, Reason} ->
                    {stop, Reason}
            end;

        {error, Reason} ->
            error_logger:error_report(
              [{module, ?MODULE},
               {line, ?LINE},
               {reason, Reason}]),
            ignore
    end.

handle_call({create_container, Detail}, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:post(Gun, "/containers/create", [{<<"content-type">>, <<"application/json">>}], jsx:encode(Detail)) => #{from => Reply, partial => <<>>}}}};

handle_call({inspect_container, ContainerId}, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:get(Gun, ["/containers/", ContainerId, "/json"]) => #{from => Reply, partial => <<>>}}}};

handle_call({start_container, ContainerId}, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:post(Gun, ["/containers/", ContainerId, "/start"], [], []) => #{from => Reply, partial => <<>>}}}};

handle_call({kill_container, ContainerId}, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:post(Gun, ["/containers/", ContainerId, "/kill"], [], []) => #{from => Reply, partial => <<>>}}}};

handle_call({remove_container, ContainerId, Volumes, Force}, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:delete(Gun, ["/containers/", ContainerId, "?v=", any:to_list(Volumes), "&force=", any:to_list(Force)]) => #{from => Reply, partial => <<>>}}}};

handle_call({logs_container, ContainerId}, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:get(Gun, ["/containers/", ContainerId, "/logs?stdout=true&stderr=true"]) => #{from => Reply, partial => <<>>}}}};

handle_call(info, Reply, #{docker := Gun, requests := Requests} = State) ->
    {noreply, State#{requests := Requests#{gun:get(Gun, "/info") => #{from => Reply, partial => <<>>}}}}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'DOWN', Monitor, process, _, normal}, #{monitor := Monitor} = State) ->
    {stop, {error, lost_connection}, State};

handle_info({gun_up, Gun, http}, #{docker := Gun} = State) ->
    {noreply, State};

handle_info({gun_down, Gun, http, _, _, _}, #{docker := Gun} = State) ->
    {noreply, State};

handle_info({gun_data, Gun, Request, fin, Data}, #{docker := Gun, requests := Requests} = State) ->
    #{Request := #{from := From, partial := Partial, headers := Headers}} = Requests,
    gen_server:reply(From, {ok, decode(Headers, <<Partial/bytes, Data/bytes>>)}),
    {noreply, State#{requests := maps:without([Request], Requests)}};

handle_info({gun_data, Gun, Request, nofin, Data}, #{docker := Gun, requests := Requests} = State) ->
    #{Request := #{partial := Partial} = Detail} = Requests,
    {noreply, State#{requests := Requests#{Request := Detail#{partial := <<Partial/bytes, Data/bytes>>}}}};

handle_info({gun_response, Gun, Request, nofin, StatusCode, Headers}, #{docker := Gun, requests := Requests} = State) ->
    #{Request := Detail} = Requests,
    {noreply, State#{requests := Requests#{Request := Detail#{status => StatusCode, headers => Headers}}}};

handle_info({gun_response, Gun, Request, fin, 204, _}, #{docker := Gun, requests := Requests} = State) ->
    #{Request := #{from := From}} = Requests,
    gen_server:reply(From, ok),
    {noreply, State#{requests := maps:without([Request], Requests)}}.


terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.


connection(undefined, _CertPath, _Cert, _Key) ->
    {error, {missing, "DOCKER_HOST"}};
connection(URI, undefined, undefined, undefined) ->
    connection(URI);
connection(_, undefined, _, undefined) ->    
    {error, {missing, "DOCKER_KEY"}};
connection(_, undefined, undefined, _) -> 
    {error, {missing, "DOCKER_CERT"}};
connection(URI, _, Cert, Key) when is_list(Cert) andalso is_list(Key) ->
    connection(URI, list_to_binary(Cert), list_to_binary(Key));
connection(URI, CertPath, undefined, undefined) ->
    case {read_file(CertPath, "cert.pem"),
          read_file(CertPath, "key.pem")} of
        
        {{ok, Cert}, {ok, Key}} ->
            connection(URI, Cert, Key);
        
        {{error, _} = Error, _} ->
            Error;
        
        {_, {error, _} = Error}->
            Error
    end.

connection(URI, Cert, Key) ->
    [{KeyType, Value, _}] = public_key:pem_decode(Key),
    [{_, Certificate, _}] = public_key:pem_decode(Cert),
    case connection(URI) of
        {ok, Details} ->
            {ok, Details#{cert => Certificate,
                          key => {KeyType, Value}}};

        {error, _} = Error ->
            Error
    end.


connection(URI) ->
    case http_uri:parse(URI) of
        {ok, {_, _, Host, Port, _, _}} ->
            {ok, #{host => Host, port => Port}};

        {error, _} = Error ->
            Error
    end.


read_file(Path, File) ->
    file:read_file(filename:join(Path, File)).



decode(Headers, Body) when is_list(Headers) ->
    decode(maps:from_list(Headers), Body);

decode(#{<<"transfer-encoding">> := <<"chunked">>} = Headers, <<1, 0, 0, 0, 0, 0, 0, Length:8, Data:Length/bytes, Remainder/bytes>>) ->
    <<Data/bytes, (decode(Headers, Remainder))/bytes>>;

decode(#{<<"content-type">> := <<"application/json">>}, JSON) ->
    jsx:decode(JSON, [return_maps]);
decode(#{}, Body) ->
    Body.

    
