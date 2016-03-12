#-*- mode: makefile-gmake -*-
# Copyright (c) 2016 Peter Morgan <peter.james.morgan@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
PROJECT = raft
PROJECT_DESCRIPTION = Raft Consensus
PROJECT_VERSION = 0.0.1

DEPS = \
	any \
	cowboy \
	crown \
	gproc \
	jsx \
	gun \
	recon \
	rfc4122

LOCAL_DEPS = \
	mnesia

dep_any = git https://github.com/shortishly/any.git master
dep_cowboy = git https://github.com/ninenines/cowboy.git 2.0.0-pre.3
dep_crown = git https://github.com/shortishly/crown.git master
dep_rfc4122 = git https://github.com/shortishly/erlang-rfc4122.git master

SHELL_OPTS = \
	-mnesia dir db \
	-s $(PROJECT)

include erlang.mk
