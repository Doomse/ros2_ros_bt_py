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
from copy import deepcopy

from ros_bt_py.vendor.result import Result, Ok, Err, do

from ros_bt_py.data_types import (
    BuiltinOrRosType,
    ReferenceListType,
    ReferenceType,
    ReferenceDictType,
    StringType,
)
from ros_bt_py.exceptions import BehaviorTreeException
from ros_bt_py.helpers import rsetattr, BTNodeState
from ros_bt_py.node import Leaf, define_bt_node
from ros_bt_py.node_config import NodeConfig


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "list_type": BuiltinOrRosType(),
            "list": ReferenceListType(reference="list_type"),
            "value": ReferenceType(reference="list_type"),
        },
        outputs={"new_list": ReferenceListType(reference="list_type")},
        max_children=0,
    )
)
class AppendListItem(Leaf):
    """Appends `item` to the end of `list`."""

    def _do_setup(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        match self.inputs.any_updated("list", "value"):
            case Err(e):
                return Err(e)
            case Ok(b):
                updated = b

        match do(
            Ok(li + [v])
            for li in self.inputs.get_value_as("list", list)
            for v in self.inputs.get_value("value")
        ):
            case Err(e):
                return Err(e)
            case Ok(li):
                out_list = li

        if updated:
            return self.outputs.set_value("new_list", out_list).and_then(
                lambda _: Ok(BTNodeState.SUCCEEDED)
            )
        return Ok(BTNodeState.SUCCEEDED)

    def _do_shutdown(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.SHUTDOWN)

    def _do_reset(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)

    def _do_untick(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "object_type": BuiltinOrRosType(),
            "attr_type": BuiltinOrRosType(),
            "attr_name": StringType(),
            "object": ReferenceType(reference="object_type"),
            "attr_value": ReferenceType(reference="attr_type"),
        },
        outputs={"new_object": ReferenceType(reference="object_type")},
        max_children=0,
    )
)
class SetAttr(Leaf):
    """Set the attribute named `attr` in `object`."""

    def _do_setup(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        match self.inputs.any_updated("object", "attr_value", "attr_name"):
            case Err(e):
                return Err(e)
            case Ok(b):
                updated = b

        def set_and_return(obj, attr, val):
            rsetattr(obj, attr, val)
            return obj

        match do(
            Ok(set_and_return(o, n, v))
            for o in self.inputs.get_value("object")
            for n in self.inputs.get_value_as("attr_name", str)
            for v in self.inputs.get_value("value")
        ):
            case Err(e):
                return Err(e)
            case Ok(o):
                obj = o

        if updated:
            return self.outputs.set_value("new_object", obj).and_then(
                lambda _: Ok(BTNodeState.SUCCEEDED)
            )
        return Ok(BTNodeState.SUCCEEDED)

    def _do_shutdown(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.SHUTDOWN)

    def _do_reset(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)

    def _do_untick(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        inputs={
            "attr_type": BuiltinOrRosType(),
            "attr_name": StringType(),
            "object": ReferenceDictType(reference="attr_type"),
            "attr_value": ReferenceType("attr_type"),
        },
        outputs={"new_object": ReferenceDictType(reference="attr_type")},
        max_children=0,
    )
)
class SetDictItem(Leaf):
    """Set the attribute named `attr` in `object`."""

    def _do_setup(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)

    def _do_tick(self) -> Result[BTNodeState, BehaviorTreeException]:
        match self.inputs.any_updated("object", "attr_value", "attr_name"):
            case Err(e):
                return Err(e)
            case Ok(b):
                updated = b

        def set_and_return(dir, key, val):
            dir[key] = val
            return dir

        match do(
            Ok(set_and_return(d, k, v))
            for d in self.inputs.get_value_as("object", dict)
            for k in self.inputs.get_value_as("attr_name", str)
            for v in self.inputs.get_value("attr_value")
        ):
            case Err(e):
                return Err(e)
            case Ok(d):
                out_dict = d

        if updated:
            return self.outputs.set_value("new_object", out_dict).and_then(
                lambda _: Ok(BTNodeState.SUCCEEDED)
            )
        return Ok(BTNodeState.SUCCEEDED)

    def _do_shutdown(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.SHUTDOWN)

    def _do_reset(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)

    def _do_untick(self) -> Result[BTNodeState, BehaviorTreeException]:
        return Ok(BTNodeState.IDLE)
