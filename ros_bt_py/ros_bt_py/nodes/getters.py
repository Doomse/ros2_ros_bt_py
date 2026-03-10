# Copyright 2023 FZI Forschungszentrum Informatik
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#
#    * Neither the name of the FZI Forschungszentrum Informatik nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
"""BT nodes to get values from containers and other nodes."""

import abc

from ros_bt_py.vendor.result import Result, Ok, Err

from ros_bt_py.data_types import (
    BlankType,
    BoolType,
    BuiltinOrRosType,
    IntType,
    ListType,
    ReferenceDictType,
    ReferenceListType,
    ReferenceType,
    StringType,
)
from ros_bt_py.exceptions import BehaviorTreeException
from ros_bt_py.helpers import BTNodeState, rgetattr
from ros_bt_py.node import Decorator, define_bt_node
from ros_bt_py.node_config import NodeConfig


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={"succeed_on_stale_data": BoolType(allow_dynamic=False)},
        outputs={},
        max_children=1,
    )
)
class Getter(Decorator):

    def _do_setup(self) -> Result[BTNodeState, BehaviorTreeException]:
        if len(self.children) == 1:
            match self.children[0].setup():
                case Err(e):
                    return Err(e)
                case Ok(_):
                    pass
        match self.inputs.get_value_as("succeed_on_stale_data", bool):
            case Err(e):
                return Err(e)
            case Ok(b):
                self.succeed_on_stale_data = b
        return Ok(BTNodeState.IDLE)

    @abc.abstractmethod
    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        # Tick child (if any) so it can produce its output before we process it
        if len(self.children) == 1:
            return self.children[0].tick()
        return Ok(BTNodeState.SUCCEEDED)
        # Subclasses have to implement their own `_do_tick` methods to do the actual work

    def _do_shutdown(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.SHUTDOWN)

    def _do_reset(self) -> Result[BTNodeState, BehaviorTreeException]:
        if len(self.children) == 1:
            return self.children[0].reset()
        return Ok(BTNodeState.IDLE)

    def _do_untick(self) -> Result[BTNodeState, BehaviorTreeException]:
        if len(self.children) == 1:
            return self.children[0].untick()
        return Ok(BTNodeState.IDLE)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "list_type": BuiltinOrRosType(),
            "index": IntType(min_value=0),
            "list": ReferenceListType(reference="list_type"),
        },
        outputs={"item": ReferenceType("list_type")},
        max_children=1,
    )
)
class GetListItem(Getter):
    """
    Extracts the item at the given `index` from `list`.

    The option parameter `succeed_on_stale_data` determines whether
    the node returns SUCCEEDED or RUNNING if `list` hasn't been
    updated since the last tick.

    """

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        # Tick child (if any) so it can produce its output before we process it
        match super()._do_tick():
            case Err(e):
                return Err(e)
            case Ok(s):
                if s in [BTNodeState.FAILED, BTNodeState.RUNNING]:
                    return Ok(s)

        match self.inputs.is_updated("list"):
            case Err(e):
                return Err(e)
            case Ok(b):
                list_updated = b
        match self.inputs.get_value_as("list", list):
            case Err(e):
                return Err(e)
            case Ok(l):
                in_list = l
        match self.inputs.get_value_as("index", int):
            case Err(e):
                return Err(e)
            case Ok(i):
                index = i

        if list_updated:
            try:
                output = in_list[index]
            except IndexError:
                self.logerr("List index %d out of bound for list %s" % (index, in_list))
                return Ok(BTNodeState.FAILED)
            match self.outputs.set_value("item", output):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    return Ok(BTNodeState.SUCCEEDED)
        else:
            if self.succeed_on_stale_data:
                # We don't need to check whether we have gotten any
                # data at all, because if we hadn't the tick method
                # would raise an error
                return Ok(BTNodeState.SUCCEEDED)
            else:
                self.loginfo("No new data since last tick!")
                return Ok(BTNodeState.RUNNING)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "value_type": BuiltinOrRosType(),
            "key": StringType(),
            "dict": ReferenceDictType(reference="value_type"),
        },
        outputs={"value": ReferenceType(reference="value_type")},
        max_children=1,
    )
)
class GetDictItem(Getter):
    """Get a item with a specific key from a dict input."""

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        # Tick child (if any) so it can produce its output before we process it
        match super()._do_tick():
            case Err(e):
                return Err(e)
            case Ok(s):
                if s in [BTNodeState.FAILED, BTNodeState.RUNNING]:
                    return Ok(s)

        match self.inputs.is_updated("dict"):
            case Err(e):
                return Err(e)
            case Ok(b):
                dict_updated = b
        match self.inputs.get_value_as("dict", dict):
            case Err(e):
                return Err(e)
            case Ok(d):
                in_dict = d
        match self.inputs.get_value_as("key", str):
            case Err(e):
                return Err(e)
            case Ok(s):
                key = s

        if dict_updated:
            try:
                output = in_dict[key]
            except KeyError:
                self.logwarn(f"Key {key} is not in dict {str(in_dict)}")
                return Ok(BTNodeState.FAILED)
            match self.outputs.set_value("value", output):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    return Ok(BTNodeState.SUCCEEDED)
        else:
            if self.succeed_on_stale_data:
                return Ok(BTNodeState.SUCCEEDED)
            else:
                self.loginfo("No new data since last tick!")
                return Ok(BTNodeState.RUNNING)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "value_type": BuiltinOrRosType(),
            "keys": ListType(element_type=StringType()),
            "dict": ReferenceDictType(reference="value_type"),
        },
        outputs={"values": ReferenceListType(reference="value_type")},
        max_children=1,
    )
)
class GetMultipleDictItems(Getter):
    """Get multiple dict items with a specific list of keys."""

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        # Tick child (if any) so it can produce its output before we process it
        match super()._do_tick():
            case Err(e):
                return Err(e)
            case Ok(s):
                if s in [BTNodeState.FAILED, BTNodeState.RUNNING]:
                    return Ok(s)

        match self.inputs.is_updated("dict"):
            case Err(e):
                return Err(e)
            case Ok(b):
                dict_updated = b
        match self.inputs.get_value_as("dict", dict):
            case Err(e):
                return Err(e)
            case Ok(d):
                in_dict = d
        match self.inputs.get_value_as("keys", list):
            case Err(e):
                return Err(e)
            case Ok(l):
                keys = l

        if dict_updated:
            try:
                outputs = [in_dict[key] for key in keys]
            except KeyError:
                self.logwarn(
                    f"One of the keys from {keys} is not in dict {str(in_dict)}"
                )
                return Ok(BTNodeState.FAILED)
            match self.outputs.set_value("values", outputs):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    return Ok(BTNodeState.SUCCEEDED)
        else:
            if self.succeed_on_stale_data:
                return Ok(BTNodeState.SUCCEEDED)
            else:
                self.loginfo("No new data since last tick!")
                return Ok(BTNodeState.RUNNING)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "attr_type": BuiltinOrRosType(),
            "attr_name": StringType(),
            "object": BlankType(),
        },
        outputs={"attr": ReferenceType("attr_type")},
        max_children=1,
    )
)
class GetAttr(Getter):
    """
    Get a specific attribute form a python object.

    This can also be done with nested attributes,
    e.g. the sec argument the timestamp of a
    std_msgs/msg/Header.msg can be extracted by using stamp.sec

    """

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        # Tick child (if any) so it can produce its output before we process it
        match super()._do_tick():
            case Err(e):
                return Err(e)
            case Ok(s):
                if s in [BTNodeState.FAILED, BTNodeState.RUNNING]:
                    return Ok(s)

        match self.inputs.is_updated("object"):
            case Err(e):
                return Err(e)
            case Ok(b):
                obj_updated = b
        match self.inputs.get_value_as("object", object):
            case Err(e):
                return Err(e)
            case Ok(o):
                in_obj = o
        match self.inputs.get_value_as("attr_name", str):
            case Err(e):
                return Err(e)
            case Ok(s):
                attr = s

        if obj_updated:
            try:
                # TODO Maybe it would be nice to allow for calling 0-argument functions this way?
                output = rgetattr(in_obj, attr)
            except AttributeError:
                self.logwarn(f"Object {in_obj} does not have attribute {attr}")
                return Ok(BTNodeState.FAILED)
            match self.outputs.set_value("attr", output):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    return Ok(BTNodeState.SUCCEEDED)
        else:
            if self.succeed_on_stale_data:
                return Ok(BTNodeState.SUCCEEDED)
            else:
                self.loginfo("No new data since last tick!")
                return Ok(BTNodeState.RUNNING)
