# -*- coding: utf-8 -*-
#
# Copyright 2021 Marcus Klang (marcus.klang@cs.lth.se)
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
"""Internal functionality for queries"""
from typing import Set, TYPE_CHECKING
if TYPE_CHECKING:
    from .model import Node, NodeFieldCollection


class NodePredicate:
    def __call__(self, *args, **kwargs)->bool:
        raise NotImplementedError

    def __and__(self, other):
        return NodePredicateAnd(self, other)

    def __or__(self, other):
        return NodePredicateOr(self, other)

    def __invert__(self):
        return NodePredicateInvert(self)


class NodeLambdaPredicate(NodePredicate):
    def __init__(self, fn, fields):
        self.fn = fn
        self.fields = fields

    def __call__(self, n):
        field_data = [n.get(fld) for fld in self.fields]
        return self.fn(*field_data)

    def __repr__(self):
        return "Lambda(%s, fields=%s)" % (repr(self.fn), repr(self.fields))


class NodePredicateOr(NodePredicate):
    def __init__(self,  lft, rgt):
        self.lft = lft
        self.rgt = rgt

    def __call__(self, *args, **kwargs):
        return self.lft(*args, **kwargs) or self.rgt(*args, **kwargs)

    def __repr__(self):
        return "Or(%s, %s)" % (repr(self.lft), repr(self.rgt))


class NodePredicateInvert(NodePredicate):
    def __init__(self,  pred):
        self.pred = pred

    def __call__(self, *args, **kwargs):
        return not self.pred(*args, **kwargs)

    def __repr__(self):
        return "Not(%s)" % repr(self.pred)


class NodePredicateAnd(NodePredicate):
    def __init__(self, lft, rgt):
        self.lft = lft
        self.rgt = rgt

    def __call__(self, *args, **kwargs):
        return self.lft(*args, **kwargs) and self.rgt(*args, **kwargs)

    def __repr__(self):
        return "And(%s, %s)" % (repr(self.lft), repr(self.rgt))


class NodeFieldPredicateContains(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", values: Set[any]):
        self.fld = field.field
        self.values = values

    def __call__(self, n: "Node"):
        return n.get(self.fld) in self.values

    def __repr__(self):
        return "({%s} is any of %s)" % (self.fld, repr(self.values))


class NodeFieldPredicateEq(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", eq):
        self.fld = field.field
        self.eq = eq

    def __call__(self, n: "Node"):
        return n.get(self.fld) == self.eq

    def __repr__(self):
        return "({%s} == %s)" % (self.fld, repr(self.eq))


class NodeFieldPredicateNeq(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", neq):
        self.fld = field.field
        self.neq = neq

    def __call__(self, n: "Node"):
        return n.get(self.fld) != self.neq

    def __repr__(self):
        return "({%s} != %s)" % (self.fld, repr(self.neq))


class NodeFieldPredicateLe(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", le):
        self.fld = field.field
        self.le = le

    def __call__(self, n: "Node"):
        return n.get(self.fld) <= self.le

    def __repr__(self):
        return "({%s} <= %s)" % (self.fld, repr(self.le))


class NodeFieldPredicateLt(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", lt):
        self.fld = field.field
        self.lt = lt

    def __call__(self, n: "Node"):
        return n.get(self.fld) < self.lt

    def __repr__(self):
        return "({%s} < %s)" % (self.fld, repr(self.lt))


class NodeFieldPredicateGe(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", ge):
        self.fld = field.field
        self.ge = ge

    def __call__(self, n: "Node"):
        return n.get(self.fld) >= self.ge

    def __repr__(self):
        return "({%s} > %s)" % (self.fld, repr(self.ge))


class NodeFieldPredicateGt(NodePredicate):
    def __init__(self, field: "NodeFieldCollection", gt):
        self.fld = field.field
        self.gt = gt

    def __call__(self, n: "Node"):
        return n.get(self.fld) > self.gt

    def __repr__(self):
        return "({%s} > %s)" % (self.fld, repr(self.gt))


class NodeFieldPredicateLambda(NodePredicate):
    def __init__(self, field, fn):
        self.field = field
        self.fn = fn

    def __call__(self, n):
        return self.fn(n.get(self.field))

    def __repr__(self):
        return "Lambda(%s, %s)" % (repr(self.field), repr(self.fn))
