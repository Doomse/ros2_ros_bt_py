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
from typing import Any

from typeguard import typechecked
import rclpy
import rclpy.logging
from ros_bt_py.data_types import DataContainer
from ros_bt_py.vendor.result import Err, Ok, Result

from ros_bt_py.custom_types import (
    FilePath,
    RosActionName,
    RosActionType,
    RosServiceName,
    RosServiceType,
    RosTopicName,
    RosTopicType,
)
from ros_bt_py.helpers import json_encode
from ros_bt_py.ros_helpers import get_interface_name
from ros_bt_py.custom_types import TypeWrapper

import array

from ros_bt_py_interfaces.msg._node_data_type import NodeDataType


@typechecked
class NodeDataMap:
    """
    Custom container class that hides :meth:`NodeData.get` and :meth:`NodeData.set`.

    Acts like a regular dict, with three caveats:

    1. All keys must be strings

    2. All values must be :class:DataContainer objects
    """

    _map: dict[str, DataContainer]

    def __init__(self):
        self._map = {}

    def add(self, key: str, value: DataContainer) -> Result[None, str]:
        """
        Add a new key value pair to the node data.

        :param basestring key: The key for the new data object
        :param NodeData value: The value of the new data object

        :raises: TypeError, KeyError
        """
        if key in self._map:
            return Err(f"Key {key} is already taken!")
        self._map[key] = value
        return Ok(None)

    def is_updated(self, key: str) -> Result[bool, str]:
        """
        Check whether the data at the given key has been updated since the last reset of `updated`.

        :param basestring key: Key for the data object whose updated status we want to check

        :rtype: bool
        """
        if key in self._map:
            return Ok(self._map[key].is_updated())
        return Err(f"There is no item with key {key}")

    def reset_updated(self) -> None:
        """Reset the `updated` property of all data in this map."""
        for key in self._map:
            self._map[key].reset_updated()

    def get_serialized_value(self, key) -> Result[str, str]:
        if key not in self._map:
            return Err(f"No member named {key}")
        return Ok(self._map[key].serialize_value())

    def get_serialized_type(self, key) -> Result[NodeDataType, str]:
        if key not in self._map:
            return Err(f"No member named {key}")
        return Ok(self._map[key].serialize_type())

    def __len__(self):
        return len(self._map)

    def __getitem__(self, key):
        if key not in self._map:
            return Err(f"No member named {key}")
        return self._map[key].get_value()

    def __setitem__(self, key, value):
        if key not in self._map:
            return Err(f"No member named {key}")
        self._map[key].set_value(value)

    def __iter__(self):
        return self._map.__iter__()

    def __contains__(self, key):
        return key in self._map

    def __eq__(self, other):
        if not isinstance(other, NodeDataMap):
            return False
        return self._map == other._map

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return f"NodeDataMap({self._map!r})"
