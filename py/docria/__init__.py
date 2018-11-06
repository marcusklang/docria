# -*- coding: utf-8 -*-
#
# Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
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

from docria.model import Document, Node, NodeList, DataTypes as T
from docria.storage import DocumentIO
from docria.codec import MsgpackCodec
from docria.printout import set_large_screen
