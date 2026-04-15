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

# from enum import StrEnum Not available in Python3.10
import abc
import functools
import re
from typing import Any, Optional, cast
from typeguard import typechecked

import rclpy
import rclpy.logging
import rosidl_runtime_py.utilities

from ros_bt_py_interfaces.msg import NodeState


# TODO Update this once StrEnum is available in all relevant versions (aka Humble is EOL)
@typechecked
# class BTNodeState(StrEnum):
class BTNodeState(abc.ABC):
    # Lots of duct tape to make type checkers happy.
    #   All of this casting and registering becomes unnecessary
    #   once we can switch to StrEnum, which behaves as expected
    UNINITIALIZED = cast("BTNodeState", NodeState.UNINITIALIZED)
    IDLE = cast("BTNodeState", NodeState.IDLE)
    UNASSIGNED = cast("BTNodeState", NodeState.UNASSIGNED)
    ASSIGNED = cast("BTNodeState", NodeState.ASSIGNED)
    RUNNING = cast("BTNodeState", NodeState.RUNNING)
    SUCCEEDED = cast("BTNodeState", NodeState.SUCCEEDED)
    FAILED = cast("BTNodeState", NodeState.FAILED)
    BROKEN = cast("BTNodeState", NodeState.BROKEN)
    PAUSED = cast("BTNodeState", NodeState.PAUSED)
    SHUTDOWN = cast("BTNodeState", NodeState.SHUTDOWN)
    # These casts are for the purpose of mypy/pylance type checking,
    #   which ignore abc `register` subclasses


BTNodeState.register(str)
# Register `str` as subclass to `BTNodeState` for the purpose of typeguard


INT_LIMITS = {
    "int8": (-(2**7), 2**7 - 1),
    "int16": (-(2**15), 2**15 - 1),
    "int32": (-(2**31), 2**31 - 1),
    "int64": (-(2**63), 2**63 - 1),
    "uint8": (0, 2**8 - 1),
    "uint16": (0, 2**16 - 1),
    "uint32": (0, 2**32 - 1),
    "uint64": (0, 2**64 - 1),
}


def int_limits_dict(name: str) -> dict[str, int]:
    limits = INT_LIMITS[name]
    return {"min_value": limits[0], "max_value": limits[1]}


FLOAT_LIMITS = {
    "float": (-3.4028235e38, 3.4028235e38),
    "double": (-1.7976931348623157e308, 1.7976931348623157e308),
}


def float_limits_dict(name: str) -> dict[str, float]:
    limits = FLOAT_LIMITS[name]
    return {"min_value": limits[0], "max_value": limits[1]}


# Max uint64 value that exactly matches a float64 value
INT_FLOAT_MAX = 2**64 - 1616


# handling nested objects,
# see https://stackoverflow.com/questions/31174295/getattr-and-setattr-on-nested-objects
def rgetattr(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split("."))


def rsetattr(obj, attr: str, val):
    pre, _, post = attr.rpartition(".")
    return setattr(rgetattr(obj, pre) if pre else obj, post, val)


def get_default_value(data_type: Any, ros: bool = False) -> Any:
    if data_type is type:
        return int
    elif data_type is int:
        return 0
    elif data_type is str:
        return "foo"
    elif data_type is float:
        return 1.2
    elif data_type is bool:
        return False
    elif data_type is bytes:
        return b"\x00"
    elif data_type is list:
        return []
    elif data_type is dict:
        return {}
    elif ros:
        return data_type()
    else:
        return {}


def build_message_field_dicts(message_object: Any) -> tuple[dict, dict]:
    """
    Thin wrapper over `get_field_values_and_types`.

    This allows passing a whole message object,
    this will then iterate over all the message object's fields.

    See the docs of `get_field_values_and_types` for details on what is returned.
    """
    default_message = message_object.__class__()
    values_dict = {}
    types_dict = {}
    for field_name, field_type in message_object.get_fields_and_field_types().items():
        values_dict[field_name], types_dict[field_name] = get_field_values_and_types(
            field_type,
            getattr(message_object, field_name),
            getattr(default_message, field_name),
        )
    return values_dict, types_dict


def get_field_values_and_types(
    field_type: str, field_value: Optional[Any], optional_default: Optional[Any] = None
) -> tuple[Any, dict]:
    """
    Recursively identify the values and types of all fields.

    Given a corresponding type string, current value and (optional) default value.
    The default_value is used to obtain custom defaults set at message definition,
    if omitted, it will be recovered where possible, and fallbacks will be used where it's not.

    The field values are returned as a simple recursive dictionary,
    ending in values of buit-in types.
    These are only valid if field_value is not None

    The field types are given as a dictionary with three keys:
        - 'own_type': the type of the field, may be one of the following:
            - a built-in type
            - a message class name
            - the special keyword 'sequence' indicating an array field
        - 'default_value': the default value of the built-in type, None otherwise
        - 'nested_type': depending on the value of own_type, this is:
            - None for built-in types
            - a (recursive) dict with field names and types for message classes
            - a dict with a type specification (this) for 'sequence'
        - 'max_length': only relevant for 'sequence' and 'string' types,
            equals -1 if it's unbounded and for all other types
        - 'is_static': specifies if the max_length of 'sequence' is forced,
            meaing the sequence has to be exactly this length. False for all other types
    """
    # Checks if the type matches a sequence definition and extracts the element-type and bounds
    match_sequence = re.match(r"sequence<([\w\/<>]+)(?:, (\d+))?>", field_type)
    # Checks if the type matches an array definition and extracts the element-type and bounds
    match_array = re.match(r"([\w\/<>]*)\[(\d+)\]", field_type)
    # Parse sequence and array types
    if match_sequence or match_array:
        # Recover defaults
        default_value = optional_default
        if optional_default is None:
            default_value = []

        nested_type_str: str
        max_len_str: str
        is_static = False
        if match_sequence:
            nested_type_str, max_len_str = match_sequence.groups()
        if match_array:
            nested_type_str, max_len_str = match_array.groups()
            is_static = True
        if max_len_str is None:
            max_len = -1
        else:
            max_len = int(max_len_str)
        _, nested_type = get_field_values_and_types(nested_type_str, None)
        nested_values = []
        if field_value is not None:
            for val in field_value:
                nval, _ = get_field_values_and_types(nested_type_str, val)
                nested_values.append(nval)
        return nested_values, {
            "own_type": field_type,
            "default_value": None,
            "nested_type": nested_type,
            "max_length": max_len,
            "is_static": is_static,
        }

    # Check if the type matches a message type
    if field_type.find("/") != -1:
        # Get default field values
        default_value = optional_default
        if optional_default is None:
            default_value = rosidl_runtime_py.utilities.get_message(field_type)()

        default_value: Any

        # Iterate fields for recursion
        nested_value = {}
        nested_type = {}
        for fname, ftype in default_value.get_fields_and_field_types().items():
            nested_value[fname], nested_type[fname] = get_field_values_and_types(
                ftype,
                getattr(field_value, fname, None),
                getattr(default_value, fname),
            )
        return nested_value, {
            "own_type": field_type,
            "default_value": None,
            "nested_type": nested_type,
            "max_length": -1,
            "is_static": False,
        }

    # Parse built-in types
    fallback_default: Any = None
    max_len = -1
    if field_type.find("bool") != -1:
        fallback_default = False
    elif (
        field_type.find("int") != -1
        or field_type.find("long") != -1
        or field_type.find("char") != -1
    ):
        fallback_default = 0
    elif field_type.find("float") != -1 or field_type.find("double") != -1:
        fallback_default = 0.0
    elif field_type.find("octet") != -1:
        fallback_default = b"\x00"
    elif field_type.find("string") != -1:
        fallback_default = ""
        match_length = re.match(r"\w+<(\d+)>", field_type)
        if match_length:
            max_len = int(match_length[1])
    else:
        rclpy.logging.get_logger("package_manager").warn(
            f"Unidentified built-in type {field_type}"
        )

    default_value = optional_default
    if optional_default is None:
        default_value = fallback_default

    return field_value, {
        "own_type": field_type,
        "default_value": default_value,
        "nested_type": None,
        "max_length": max_len,
        "is_static": False,
    }
