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

from docria.model import Document, Node
from typing import List, Dict


class EdgeIndex:
    """Precomputed index of all links in the Document graphs for a given set of layers"""
    def __init__(self, doc, *layer_names):
        self.doc = doc
        self.layer_names = layer_names
        self.in_index = {}
        self.out_index = {}

    def build_index(self):
        pass

    def update(self, n: Node):
        self.remove(n)
        self.add(n)

    def remove(self, n: Node):
        pass

    def add(self, n: Node):
        pass

    def sources(self, node: Node)->Dict[Node, str]:
        """Get nodes pointing to give node"""
        pass

    def targets(self, node: Node)->Dict[str, Node]:
        """Get nodes this node is pointing to"""
        pass

