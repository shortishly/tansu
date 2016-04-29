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


-module(raft_api_client_locks_resource).
-export([init/2]).
-export([info/3]).
-export([terminate/3]).


init(Req, _) ->
    case raft_consensus:info() of
        #{leader := _} ->
	    Headers = [{<<"content-type">>, <<"text/event-stream">>},
		       {<<"cache-control">>, <<"no-cache">>}],
            raft_api:kv_subscribe(cowboy_req:path_info(Req)),

            Lock = cowboy_req:path_info(Req),
            Key = raft_uuid:new(),
            
            %% try and lock with our key:
            case raft_api:kv_test_and_set(Lock, undefined, Key) of
                not_leader ->
                    %% whoa, we were the leader.
                    service_unavailable(Req, #{});

                _ ->
                    {cowboy_loop,
                     cowboy_req:chunked_reply(200, Headers, Req),
                     #{lock => Lock, key => Key, n => 1}}
            end;

        #{follower := #{connections := Connections, leader := Leader}} ->
            case Connections of
                #{Leader := #{host := Host, port := Port}} ->
                    raft_api_client_proxy_resource:init(
                      Req, #{host => binary_to_list(Host), port => Port});

                #{} ->
                    service_unavailable(Req, #{})
            end;

        #{} ->
            %% Neither a leader nor a follower with an established
            %% leader then the service is unavailable.
            service_unavailable(Req, #{})
    end.

info(#{event := set, data := #{value := Key}, module := raft_sm}, Req, #{key := Key, n := N} = State) ->
    {chunk(N, granted, Req), Req, State#{n := N+1}};

info(#{event := set, data := #{value := Key}, module := raft_sm}, Req, #{n := N} = State) ->
    {chunk(N, not_granted, #{locked_by => Key}, Req), Req, State#{n := N+1}};

info(#{event := deleted, module := raft_sm}, Req, #{lock := Lock, key := Key, n := N} = State) ->
    case raft_api:kv_test_and_set(Lock, undefined, Key) of
        not_leader ->
            %% whoa, we were the leader.
            service_unavailable(Req, #{});

        _ ->
            {chunk(N, not_granted, #{action => retrying}, Req), Req, State#{n := N+1}}
    end;

info(#{event := Event, data := Data, module := raft_sm}, Req, #{n := N} = State) ->
    {chunk(N, Event, Data, Req), Req, State#{n := N+1}};

info(Event, Req, #{proxy := Proxy} = State) ->
    Proxy:info(Event, Req, State).

terminate(Reason, Req, #{proxy := Proxy} = State) ->
    Proxy:terminate(Reason, Req, State);

terminate(_Reason, _Req, #{lock := Lock, key := Key}) ->
    raft_api:kv_test_and_delete(Lock, Key),
    ok.


chunk(Id, Event, Data, Req) ->
    cowboy_req:chunk(
      ["id: ",
       any:to_list(Id),
       "\nevent: ",
       any:to_list(Event),
       "\ndata: ",
       jsx:encode(Data), "\n\n"],
      Req).


chunk(Id, Event, Req) ->
    cowboy_req:chunk(
      ["id: ",
       any:to_list(Id),
       "\nevent: ",
       any:to_list(Event),
       "\n\n"],
      Req).



                
service_unavailable(Req, State) ->
    stop_with_code(503, Req, State).

stop_with_code(Code, Req, State) ->
    {stop, cowboy_req:reply(Code, Req), State}.


            
            

