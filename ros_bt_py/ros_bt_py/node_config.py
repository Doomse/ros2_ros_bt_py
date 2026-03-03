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
from typing import Dict, Optional, List

from typeguard import typechecked

from ros_bt_py.vendor.result import Result, Err, Ok

from ros_bt_py.data_types import DataContainer
from ros_bt_py.exceptions import NodeConfigError


@typechecked
class NodeConfig(object):

    def __init__(
        self,
        inputs: Dict[str, DataContainer],
        outputs: Dict[str, DataContainer],
        max_children: Optional[int],
        version: str = "",
        tags: Optional[List[str]] = None,
    ):
        """
        Describe the interface of a :class:ros_bt_py.node.Node .

        :type inputs Dict[str, DataContainer]
        :param inputs:

        Map from input names to their data types.

        :type outputs Dict[str, DataContainer]
        :param outputs:

        Map from output names to their data types.

        :type max_children: int or None
        :param max_children:

        The maximum number of children this node type is allowed to
        have.  If this is None, the node type supports any amount of
        children.
        """
        self.inputs = inputs
        self.outputs = outputs
        self.max_children = max_children
        self.version = version

        if tags is None:
            tags = []
        self.tags = tags

    def __repr__(self) -> str:
        return (
            "NodeConfig("
            f"inputs={self.inputs}, "
            f"outputs={self.outputs}, "
            f"max_children={self.max_children}, "
            f"version={self.version})"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, NodeConfig):
            return False
        return (
            self.inputs == other.inputs
            and self.outputs == other.outputs
            and self.max_children == other.max_children
            and self.version == other.version
        )

    def __ne__(self, other) -> bool:
        return not self == other

    def extend(self, other: "NodeConfig") -> Result[None, NodeConfigError]:
        """
        Extend the input, output and option dicts with values from `other`.

        :raises: KeyError, ValueError
          If any of the dicts in `other` contains keys that already exist,
          raise `KeyError`. If `max_children` has a value different from ours,
          raise `ValueError`.
        """
        if self.max_children != other.max_children:
            return Err(
                NodeConfigError(
                    f"Mismatch in max_children: {self.max_children} vs {other.max_children}"
                )
            )
        duplicate_inputs = []
        for key in other.inputs:
            if key in self.inputs:
                duplicate_inputs.append(key)
                continue
            self.inputs[key] = other.inputs[key]
        duplicate_outputs = []
        for key in other.outputs:
            if key in self.outputs:
                duplicate_outputs.append(key)
                continue
            self.outputs[key] = other.outputs[key]

        if duplicate_inputs or duplicate_outputs:
            msg = "Duplicate keys: "
            keys_strings = []
            if duplicate_inputs:
                keys_strings.append(f"inputs: {str(duplicate_inputs)}")
            if duplicate_outputs:
                keys_strings.append(f"outputs: {str(duplicate_outputs)}")
            msg += ", ".join(keys_strings)
            return Err(NodeConfigError(msg))
        return Ok(None)
