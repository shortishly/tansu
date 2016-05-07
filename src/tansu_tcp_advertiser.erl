%% Copyright (c) 2012-2016 Peter Morgan <peter.james.morgan@gmail.com>
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

-module(tansu_tcp_advertiser).

-export([instances/0]).
-export([service/0]).

service() ->
    "_tansu._tcp".

instances() ->
    {ok, Hostname} = inet:gethostname(),
    Id = any:to_list(tansu_consensus:id()),
    [#{hostname => Hostname,
       port => tansu_config:port(http),
       instance => instance(Id, Hostname),
       properties => #{host => net_adm:localhost(),
                       env => tansu_config:environment(),
                       id => tansu_consensus:id(),
                       la => tansu_consensus:last_applied(),
                       ci => tansu_consensus:commit_index(),
                       vsn => tansu:vsn()},
       priority => 0,
       weight => 0}].


instance(Node, Hostname) ->
    Node ++ "@" ++ Hostname ++ "." ++ service() ++ mdns_config:domain().
