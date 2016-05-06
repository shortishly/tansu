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


-module(raft_api_locks_resource).
-export([init/2]).
-export([info/3]).
-export([terminate/3]).


init(Req, _) ->
    case raft_consensus:info() of
        #{leader := _} ->
            self() ! try_lock,
            raft_api:kv_subscribe(cowboy_req:path_info(Req)),
            do_ping(),
            Key = raft_uuid:new(),
            Headers = [{<<"content-type">>, <<"text/event-stream">>},
                       {<<"cache-control">>, <<"no-cache">>},
                       {<<"key">>, Key}],
            {cowboy_loop,
             cowboy_req:chunked_reply(200, Headers, Req),
             #{lock => cowboy_req:path_info(Req),
               key => Key,
               n => 1}};

        #{follower := #{connections := Connections, leader := #{id := Leader}}} ->
            case Connections of
                #{Leader := #{host := Host, port := Port}} ->
                    %% We are connected to a follower with an
                    %% established leader, proxy this request through
                    %% to the leader.
                    raft_api_proxy_resource:init(
                      Req, #{host => binary_to_list(Host), port => Port});

                #{} ->
                    service_unavailable(Req, #{})
            end;

        #{} ->
            %% Neither a leader nor a follower with an established
            %% leader then the service is unavailable.
            service_unavailable(Req, #{})
    end.

info(try_lock, Req, State) ->
    do_try_lock(Req, State);

info(#{event := set, data := #{value := Key}, module := raft_sm}, Req, #{key := Key, n := N} = State) ->
    %% The lock has been granted, by setting the value to our key.
    {ok, Req, State#{n := N+1}};

info(#{event := set, data := #{value := Key}, module := raft_sm}, Req, #{n := N} = State) ->
    %% The lock has been granted to someone else by setting its value
    %% to their key.
    raft_stream:chunk(N, not_granted, #{locked_by => Key}, Req),
    {ok, Req, State#{n := N+1}};

info(#{event := deleted, module := raft_sm}, Req, State) ->
    %% The lock has been deleted, try and obtain the lock if there is
    %% also a leader.
    do_try_lock(Req, State);

info(ping, Req, #{n := N} = State) ->
    raft_stream:chunk(N, ping, Req),
    do_ping(),
    {ok, Req, State#{n := N+1}};

info(Event, Req, #{proxy := Proxy} = State) ->
    Proxy:info(Event, Req, State).

terminate(Reason, Req, #{proxy := Proxy} = State) ->
    Proxy:terminate(Reason, Req, State);

terminate(_Reason, _Req, #{lock := Lock, key := Key}) ->
    raft_api:kv_test_and_delete(Lock, Key),
    ok.

do_ping() ->
    erlang:send_after(raft_config:timeout(stream_ping), self(), ping).

do_try_lock(Req, #{lock := Lock, key := Key, n := N} = State) ->
    case raft_api:kv_test_and_set(Lock, undefined, Key) of
        not_leader ->
            %% whoa, we were the leader.
            raft_stream:chunk(N, not_granted, #{service_unavailable => not_leader}, Req),
            {stop, Req, State#{n := N+1}};

        ok ->
            raft_stream:chunk(N, granted, Req),
            {ok, Req, State#{n := N+1}};

        error ->
            raft_stream:chunk(N, not_granted, Req),
            {ok, Req, State#{n := N+1}}
    end.
                
service_unavailable(Req, State) ->
    stop_with_code(503, Req, State).

stop_with_code(Code, Req, State) ->
    {stop, cowboy_req:reply(Code, Req), State}.


            
            

