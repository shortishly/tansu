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
PROJECT = tansu
PROJECT_DESCRIPTION = Tansu
PROJECT_VERSION = 0.21.0

DEPS = \
	cowboy \
	crown \
	envy \
	gproc \
	gun \
	jsx \
	mdns \
	recon \
	rfc4122 \
	shelly

LOCAL_DEPS = \
	crypto \
	inets \
	mnesia \
	sasl

dep_cowboy = git https://github.com/ninenines/cowboy.git 2.0.0-pre.3
dep_crown = git https://github.com/shortishly/crown.git master
dep_envy = git https://github.com/shortishly/envy.git master
dep_mdns = git https://github.com/shortishly/mdns.git master
dep_rfc4122 = git https://github.com/shortishly/rfc4122.git master
dep_shelly = git https://github.com/shortishly/shelly.git master

SHELL_OPTS = \
	-boot start_sasl \
	-config dev.config \
	-mnesia dir db \
	-name $(PROJECT) \
	-s $(PROJECT) \
	-s rb \
	-s sync \
	-setcookie $(PROJECT)

SHELL_DEPS = \
	sync

TEST_DEPS = \
	triq

include erlang.mk
