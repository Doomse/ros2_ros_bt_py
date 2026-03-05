# Copyright 2026 FZI Forschungszentrum Informatik
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
import abc
from copy import deepcopy
from importlib import import_module
from inspect import getmodule
import json
import re
from types import NoneType
from typing import Any, Generic, Optional, Self, TypeGuard, TypeVar
from typeguard import typechecked

from ros_bt_py.ros_helpers import get_interface_name

import rosidl_runtime_py
import rosidl_runtime_py.utilities

from ros_bt_py.vendor.result import Result, Ok, Err

from ros_bt_py_interfaces.msg import NodeDataType

from example_interfaces import msg, srv, action


ANY = TypeVar("ANY")


class DataContainer(Generic[ANY], abc.ABC):
    # This type_identifier has to be assigned a value in subclass definitions
    #   and should ALWAYS be static.
    type_identifier: int

    allow_dynamic: bool
    allow_static: bool
    is_static: bool
    _value: Optional[ANY] = None
    _updated: bool

    @typechecked
    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = True,
        is_static: bool | None = None,
        value: Optional[ANY] = None,
    ) -> None:
        # This method calls `set_value` with the given value,
        #   so subclasses need to ensure that method is functional
        #   before calling `super().__init__`.
        # This also raises `ValueError` if the given value can't be assigned
        super().__init__()

        if not hasattr(self, "type_identifier"):
            raise RuntimeError(
                "All concrete implementations have to specify a type_identifier"
            )

        if not allow_dynamic and not allow_static:
            raise RuntimeError(
                "Need to allow either static or dynamic value assignment"
            )

        self.allow_dynamic = allow_dynamic
        self.allow_static = allow_static

        if is_static is None:
            self.is_static = self.allow_static
        else:
            self.is_static = is_static

        if self.is_static and not self.allow_static:
            raise RuntimeError("Cannot be static when static is not allowed")
        if not self.is_static and not self.allow_dynamic:
            raise RuntimeError("Cannot be dynamic when dynamic is not allowed")

        self.reset_updated()
        if value is not None:
            match self.set_value(value):
                case Err(e):
                    raise ValueError(e)
                case Ok(None):
                    pass

    @classmethod
    @abc.abstractmethod
    @typechecked
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        """
        Subclasses should extend this to add their specific configs
        """
        return Ok(
            {
                "allow_dynamic": msg.allow_dynamic,
                "allow_static": msg.allow_static,
                "is_static": msg.is_static,
                "value": None,
            }
        )

    @classmethod
    @typechecked
    def from_msg(cls, msg: NodeDataType) -> Result[Self, str]:
        """
        This does only set all type information, not the value.
        You need to call `deserialize_value` for that.
        """
        if not hasattr(cls, "type_identifier"):
            raise NotImplementedError("Called on abstract base class")
        if msg.type_identifier != cls.type_identifier:
            return Err("Wrong type identifier")
        match cls._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(d):
                obj = cls(**d)
        return Ok(obj)

    @abc.abstractmethod
    @typechecked
    def is_compatible(self, other: "DataContainer") -> TypeGuard[Self]:
        """
        Check if the given container is compatible with this one,
            meaning that its constraints are at least as narrow as the ones in `self`.
        This is used to compare configs given on specific nodes
            with the baseline given on the class config.
        This can also serve as a type guard for `other`,
            because the checks performed are more strict than simple type equality.

        Subclasses should extend this with additional checks where applicable.
        """
        if not isinstance(other, self.__class__):
            return False
        if self.allow_dynamic and not other.allow_dynamic:
            return False
        if self.allow_static and not other.allow_static:
            return False
        # Don't compare `is_static`, since it serves as a default value not a constraint
        return True

    @abc.abstractmethod
    def serialize_type(self) -> NodeDataType:
        type_msg = NodeDataType(
            type_identifier=self.type_identifier,
            allow_dynamic=self.allow_dynamic,
            allow_static=self.allow_static,
            is_static=self.is_static,
        )
        return type_msg

    def get_runtime_type(self) -> Self:
        """
        Returns the own runtime type, this is almost always just `self`,
        except for instances of `ReferenceContainer`.
        """
        return self

    @abc.abstractmethod
    @typechecked
    def set_value(self, value: ANY) -> Result[None, str]:
        """
        Subclasses should validate and clean incoming values
            before calling `super().set_value` to assign them.
        """
        if self.is_static and self._updated:
            return Err("Static value was already assigned")
        self._value = value
        self._updated = True
        return Ok(None)

    @typechecked
    def get_value(self) -> Result[ANY, None]:
        """
        Returns an empty `Err` if the value is `None`.
        """
        if self._value is None:
            return Err(None)
        return Ok(self._value)

    T = TypeVar("T")

    @typechecked
    def get_value_as(self, type_: type[T]) -> Result[T, Any]:
        match self.get_value():
            case Err(None):
                return Err(None)
            case Ok(v):
                value = v
        if isinstance(value, type_):
            return Ok(value)
        return Err(value)

    def reset_value(self):
        self._value = None

    def is_updated(self) -> bool:
        return self._updated

    def reset_updated(self) -> None:
        if not self.is_static:
            self._updated = False

    @typechecked
    def serialize_value(self) -> str:
        match self.get_value():
            case Err(None):
                return ""
            case Ok(v):
                return self._serialize_value(v)

    @abc.abstractmethod
    def _serialize_value(self, value: ANY) -> str:
        raise NotImplementedError("Can't serialize a base class value")

    @abc.abstractmethod
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        """This should also call `set_value` to assign the value and confirm it is valid."""
        raise NotImplementedError("Can't deserialize a base class value")


# This list gets populated through the `register_io_type` decorator,
#   and is used to identify the type of any `NodeDataType` message.
CONCRETE_IO_TYPES: list[type[DataContainer]] = []


CONTAINER = TypeVar("CONTAINER", bound=DataContainer)


@typechecked
def register_io_type(cls: type[CONTAINER]) -> type[CONTAINER]:
    """
    This decorator is only relevant for discovery from `NodeDataType`
    """
    if not hasattr(cls, "type_identifier"):
        raise RuntimeError("Registered IO types require a type identifier")
    CONCRETE_IO_TYPES.append(cls)
    return cls


@typechecked
def get_iotype_for_msg(msg: NodeDataType) -> Result[DataContainer, str]:
    """
    Note that this only constructs the data type, you still have to call
    `deserialize_value` if you want to parse the value that was passed in.
    """
    for io_type in CONCRETE_IO_TYPES:
        match io_type.from_msg(msg):
            case Err(_):
                continue  # Try all io types
            case Ok(io):
                return Ok(io)
    return Err("There is no IO type matching this identifier.")


@typechecked
def get_iotype_for_type(type_: type) -> Result[type[DataContainer], str]:
    """
    Get the IO type corresponding to the given type,
    which has to either be one of the types in `BUILTIN_TYPE_MAP` or a ROS message type.
    """
    if rosidl_runtime_py.utilities.is_message(type_):
        match get_ros_msg_type(type_):
            case Err(e):
                return Err(e)
            case Ok(c):
                return Ok(c)
    container_type = BUILTIN_TYPE_MAP.get(type_)
    if container_type is None:
        return Err(f"There's no IO type associated with {type_}")
    return Ok(container_type)


# The inheritence here is only pro-forma to typecheck the constructor.
#   It is recommended that implementations specify a container explicitly
#   E.g. `class ValueType[V](TypeContainerMixin, DataContainer[V])`
class TypeContainerMixin(DataContainer[type]):

    # Update the defaults and verify that a type container doesn't allow dynamic values
    @typechecked
    def __init__(
        self,
        allow_dynamic=False,
        allow_static=True,
        *args,
        **kwargs,
    ) -> None:
        if allow_dynamic:
            raise RuntimeError(
                "Type containers cannot be dynamic, since they're (potentially) reference targets."
            )
        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            *args,
            **kwargs,
        )

    @abc.abstractmethod
    def get_value_field(self) -> Result[type[DataContainer], None]:
        raise NotImplementedError("Can't get value field for base class")


BUILTIN = TypeVar("BUILTIN", bool, int, float, str, list, dict, bytes)


class BuiltinContainer(DataContainer[BUILTIN]):
    _type: type[BUILTIN]
    _value: BUILTIN

    @typechecked
    def set_value(self, value: BUILTIN) -> Result[None, str]:
        if not isinstance(value, self._type):
            return Err(f"Given value {value} isn't of type {self._type}")
        return super().set_value(value)

    def get_value(self) -> Result[BUILTIN, None]:
        return super().get_value()

    @typechecked
    def _serialize_value(self, value: BUILTIN) -> str:
        # Replace all non-serializeable values with ""
        #   This should only happen for untyped lists and dicts
        # Also skip all non-serializeable dict keys
        return json.dumps(
            obj=value,
            skipkeys=True,
            default=lambda _: "",
        )

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        value = json.loads(ser_value)
        return self.set_value(value)


@register_io_type
class BoolType(BuiltinContainer[bool]):
    """
    This type holds a simple boolean value
    """

    type_identifier = NodeDataType.BOOL_TYPE
    _type = bool
    _value = False

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        # Nothing to add for bool
        return super()._dict_from_msg(msg)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        # Nothing to add for bool
        return super().is_compatible(other)

    def serialize_type(self) -> NodeDataType:
        # Nothing to add for bool
        return super().serialize_type()


NUM = TypeVar("NUM", int, float)


class NumericContainer(BuiltinContainer[NUM]):
    min_value: NUM
    max_value: NUM
    lower_limit: NUM
    upper_limit: NUM

    @typechecked
    def __init__(
        self,
        min_value: Optional[NUM] = None,
        max_value: Optional[NUM] = None,
        *args,
        **kwargs,
    ) -> None:
        self.min_value = min_value if min_value else self.lower_limit
        self.max_value = max_value if max_value else self.upper_limit

        super().__init__(*args, **kwargs)

    @typechecked
    def set_value(self, value: NUM) -> Result[None, str]:
        if value < self.min_value:
            return Err(f"Given value {value} is smaller than minimum {self.min_value}")
        if value > self.max_value:
            return Err(f"Given value {value} is larger than maximum {self.max_value}")
        return super().set_value(value)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        if other.min_value < self.min_value:
            return False
        if other.max_value > self.max_value:
            return False
        return True


@register_io_type
class IntType(NumericContainer[int]):
    """
    This type holds a (64-bit) integer,
    which can optionally be constrained by upper and lower limits.
    """

    type_identifier = NodeDataType.INT_TYPE
    _type = int
    _value = 0
    lower_limit = -(2**63)
    upper_limit = 2**63 - 1

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        config["min_value"] = msg.int_min_value
        config["max_value"] = msg.int_max_value
        return Ok(config)

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.int_min_value = self.min_value
        type_msg.int_max_value = self.max_value
        return type_msg


@register_io_type
class FloatType(NumericContainer[float]):
    """
    This type holds a double,
    which can optionally be constrained by upper and lower limits.
    """

    type_identifier = NodeDataType.FLOAT_TYPE
    _type = float
    lower_limit = -1.7976931348623158e308
    upper_limit = 1.7976931348623158e308

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        config["min_value"] = msg.float_min_value
        config["max_value"] = msg.float_max_value
        return Ok(config)

    def set_value(self, value: float | int) -> Result[None, str]:
        # Silently convert int to float
        if isinstance(value, int):
            value = float(value)
        return super().set_value(value)

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.float_min_value = self.min_value
        type_msg.float_max_value = self.max_value
        return type_msg


STRING = TypeVar("STRING", str, bytes)


class StringContainer(BuiltinContainer[STRING]):
    max_length: int = 2**64 - 1
    strict_length: bool = False

    def __init__(
        self,
        max_length: Optional[int] = None,
        strict_length: Optional[bool] = None,
        *args,
        **kwargs,
    ) -> None:
        if max_length is not None:
            self.max_length = max_length
        if strict_length is not None:
            self.strict_length = strict_length

        super().__init__(*args, **kwargs)

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        config["max_length"] = msg.string_max_length
        config["strict_length"] = msg.string_strict_length
        return Ok(config)

    @typechecked
    def set_value(self, value: STRING) -> Result[None, str]:
        if len(value) > self.max_length:
            return Err(
                f"Value {value} has more items than the limit of {self.max_length}"
            )
        if self.strict_length and len(value) < self.max_length:
            return Err(
                f"Value {value} has fewer items than the limit of {self.max_length}"
            )
        return super().set_value(value)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        if other.max_length > self.max_length:
            return False
        if not other.strict_length and self.strict_length:
            return False
        return True

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.string_max_length = self.max_length
        type_msg.string_strict_length = self.strict_length
        return type_msg


@register_io_type
class StringType(StringContainer[str]):
    """
    This type holds a string, which can optionally be limited by length.
    """

    type_identifier = NodeDataType.STRING_TYPE
    _type = str
    _value = ""


@register_io_type
class PathType(StringContainer[str]):
    """
    This type holds a path uri, which has to start with 'file://' or 'package://'
    """

    type_identifier = NodeDataType.PATH_TYPE
    _type = str
    _value = ""

    @typechecked
    def set_value(self, value: str) -> Result[None, str]:
        if not value.startswith("file://") and not value.startswith("package://"):
            return Err(f"Value {value} is not a valid file or package uri")
        return super().set_value(value)


@register_io_type
class BytesType(StringContainer[bytes]):
    """
    This type holds a bytes object, which can optionally be constrained by length.
    This length restriction is set to 1 (strict) by default,
    since the bytes type is mostly used to fill 'byte' fields in ROS messages,
    which only take one byte.
    Bytes are serializes as hex strings.
    """

    type_identifier = NodeDataType.BYTES_TYPE
    _type = bytes
    _value = b"\x00"

    max_length = 1
    strict_length = True

    @typechecked
    def _serialize_value(self, value: bytes) -> str:
        return value.hex(" ")

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        try:
            value = bytes.fromhex(ser_value)
        except ValueError:
            return Err(f"Given string {ser_value} isn't valid hexcode")
        return self.set_value(value)


ITER = TypeVar("ITER", list, dict)


class IterableContainer(BuiltinContainer[ITER]):
    """
    Note that the static/dynamic attributes on element types are ignored,
    those have to be specified on the iterable itself.

    If the element type is omitted, all kinds of values are accepted,
    but value types that can't be serialized as json will silently be
    replaced with "" in any serialized output.

    Since iterables are usually mutable, this container applies `deepcopy`
    operations on every `get_value` and `set_value` to avoid accidental modification.
    """

    _element_type: Optional[DataContainer]
    max_length: int = 2**64 - 1
    strict_length: bool = False

    @typechecked
    def __init__(
        self,
        element_type: Optional[DataContainer],
        max_length: Optional[int] = None,
        strict_length: Optional[bool] = None,
        *args,
        **kwargs,
    ) -> None:
        self._element_type = element_type
        # Force element_type to accept dynamic values
        if self._element_type is not None:
            self._element_type.allow_dynamic = True
            self._element_type.is_static = False

        if max_length is not None:
            self.max_length = max_length
        if strict_length is not None:
            self.strict_length = strict_length

        super().__init__(*args, **kwargs)

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        if (
            len(msg.value_type_identifier) == 0
            or len(msg.iterable_max_length) == 0
            or len(msg.iterable_strict_length) == 0
        ):
            return Err(
                f"Message {msg} is for an iterable but doesn't specify proper constraints"
            )
        inner_msg = deepcopy(msg)
        inner_msg.type_identifier = msg.value_type_identifier[0]
        inner_msg.value_type_identifier = msg.value_type_identifier[1:]
        inner_msg.iterable_max_length = msg.iterable_max_length[1:]
        inner_msg.iterable_strict_length = msg.iterable_strict_length[1:]  # type: ignore
        match get_iotype_for_msg(inner_msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config["element_type"] = c
        config["iterable_max_length"] = msg.iterable_max_length[0]
        config["iterable_strict_length"] = msg.iterable_strict_length[0]  # type: ignore
        return Ok(config)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        return self._element_type != other._element_type

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        if self._element_type is None:
            type_msg.value_type_identifier = [NodeDataType.UNDEFINED_TYPE]
            type_msg.iterable_max_length = [self.max_length]
            type_msg.iterable_strict_length = [self.strict_length]
            return type_msg
        # Overlay msg of iterable type on msg of element type
        inner_type_msg = self._element_type.serialize_type()
        inner_type_msg.allow_dynamic = type_msg.allow_dynamic
        inner_type_msg.allow_static = type_msg.allow_static
        inner_type_msg.value_type_identifier = [inner_type_msg.type_identifier].extend(
            inner_type_msg.value_type_identifier
        )
        inner_type_msg.type_identifier = type_msg.type_identifier
        inner_type_msg.iterable_max_length = [self.max_length].extend(
            inner_type_msg.iterable_max_length
        )
        inner_type_msg.iterable_strict_length = [self.strict_length].extend(
            inner_type_msg.iterable_strict_length
        )
        return inner_type_msg

    @abc.abstractmethod
    @typechecked
    def set_value(self, value: ITER) -> Result[None, str]:
        if len(value) > self.max_length:
            return Err(
                f"Value {value} has more items than the limit of {self.max_length}"
            )
        if self.strict_length and len(value) < self.max_length:
            return Err(
                f"Value {value} has fewer items than the limit of {self.max_length}"
            )
        return super().set_value(deepcopy(value))

    def get_value(self) -> Result[ITER, None]:
        match super().get_value():
            case Err(None):
                return Err(None)
            case Ok(v):
                return Ok(deepcopy(v))


@register_io_type
class ListType(IterableContainer[list[Any]]):
    """
    This type holds a list whose values can optionally be typed by
    providing an `element_type` in the constructor.

    A typed list also uses the serialization of that element type.

    This list can optionally be constrained by length.
    """

    type_identifier = NodeDataType.LIST_TYPE
    _type = list[Any]
    _value = []

    @typechecked
    def set_value(self, value: list) -> Result[None, str]:
        if self._element_type is None:
            return super().set_value(value)
        for item in value:
            match self._element_type.set_value(item):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    pass
        return super().set_value(value)

    @typechecked
    def _serialize_value(self, value: list[Any]) -> str:
        if self._element_type is None:
            return super()._serialize_value(value)
        serialized_list = []
        for item in value:
            match self._element_type.set_value(item):
                # Since these are internal values, they should NEVER be invalid
                #   we checked them on assignment.
                case Err(_):
                    ser_item = ""
                case Ok(None):
                    ser_item = self._element_type.serialize_value()
            serialized_list.append(ser_item)
        return super()._serialize_value(serialized_list)

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        if self._element_type is None:
            return super().deserialize_value(ser_value)
        value_list = json.loads(ser_value)
        if not isinstance(value_list, self._type):
            return Err(f"Value {ser_value} is not a list")
        value = []
        for item in value_list:
            match self._element_type.deserialize_value(json.dumps(item)):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    pass
            match self._element_type.get_value():
                case Err(None):
                    # This should NEVER happen after a successful deserialization
                    value.append(None)
                case Ok(v):
                    value.append(v)
        return self.set_value(value)


@register_io_type
class DictType(IterableContainer[dict[str, Any]]):
    """
    This type holds a dict whose values can optionally be typed by
    providing an `element_type` in the constructor.
    The keys of a dict will always be converted by using `str(...)`.

    A typed dict also uses the serialization of that element type.
    """

    type_identifier = NodeDataType.DICT_TYPE
    _type = dict[str, Any]
    _value = {}

    @typechecked
    def set_value(self, value: dict) -> Result[None, str]:
        cleaned_keys = {str(k): v for k, v in value.items()}
        if self._element_type is None:
            return super().set_value(cleaned_keys)
        for item in cleaned_keys.values():
            match self._element_type.set_value(item):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    pass
        return super().set_value(cleaned_keys)

    @typechecked
    def _serialize_value(self, value: dict[str, Any]) -> str:
        if self._element_type is None:
            return super()._serialize_value(value)
        serialized_dict: dict[str, str] = {}
        for key, item in value.items():
            match self._element_type.set_value(item):
                # Since these are internal values, they should NEVER be invalid
                #   we checked them on assignment.
                case Err(_):
                    ser_item = ""
                case Ok(None):
                    ser_item = self._element_type.serialize_value()
            serialized_dict[key] = ser_item
        return super()._serialize_value(serialized_dict)

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        if self._element_type is None:
            return super().deserialize_value(ser_value)
        value_dict = json.loads(ser_value)
        if not isinstance(value_dict, self._type):
            return Err(f"Value {ser_value} is not a list")
        value = {}
        for key, item in value_dict.items():
            match self._element_type.deserialize_value(json.dumps(item)):
                case Err(e):
                    return Err(e)
                case Ok(None):
                    pass
            match self._element_type.get_value():
                case Err(None):
                    # This should NEVER happen after a successful deserialization
                    value[key] = None
                case Ok(v):
                    value[key] = v
        return self.set_value(value)


BUILTIN_TYPE_MAP: dict[type, type[DataContainer]] = {
    bool: BoolType,
    int: IntType,
    float: FloatType,
    str: StringType,
    list: ListType,
    dict: DictType,
    bytes: BytesType,
}


@typechecked
def serialize_class(cls: type) -> str:
    type_name = cls.__name__
    module = getmodule(cls)
    module_name = module.__name__ + "." if module is not None else ""
    return module_name + type_name


@typechecked
def deserialize_class(ser_cls: str) -> Result[type, str]:
    *mod_path, cls_name = ser_cls.split(".")
    try:
        module = import_module(".".join(mod_path))
    except ValueError:
        return Err(f"Type {ser_cls} has an invalid module path")
    cls = getattr(module, cls_name, None)
    if cls is None:
        return Err(f"Type {ser_cls} can't be found")
    return Ok(cls)


@register_io_type
class BuiltinType(TypeContainerMixin, DataContainer[type]):
    """
    This holds a builtin type from the `BUILTIN_TYPE_MAP` keys,
    which can optionally be a constrained further by supplying a list of valid types.
    """

    type_identifier = NodeDataType.BUILTIN_TYPE
    _value = int
    valid_types: list[type]

    @typechecked
    def __init__(
        self,
        valid_types: list[type] = list(BUILTIN_TYPE_MAP.keys()),
        *args,
        **kwargs,
    ) -> None:
        for type_ in valid_types:
            if type_ not in BUILTIN_TYPE_MAP.keys():
                raise RuntimeError(f"Type {type_} is not usable as an IO type")
        self.valid_types = valid_types
        super().__init__(*args, **kwargs)

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        valid_types: list[type] = []
        for option in msg.serialized_value_options:
            match deserialize_class(option):
                case Err(_):
                    continue
                case Ok(c):
                    valid_types.append(c)
        config["valid_types"] = valid_types
        return Ok(config)

    @typechecked
    def set_value(self, value: type) -> Result[None, str]:
        if value not in self.valid_types:
            return Err(
                f"Type {value} is a valid type. Valid types: {list(self.valid_types)}"
            )
        return super().set_value(value)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        for type_elem in other.valid_types:
            if type_elem not in self.valid_types:
                return False
        return True

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.serialized_value_options = []
        for type_elem in self.valid_types:
            type_msg.serialized_value_options.append(serialize_class(type_elem))
        return type_msg

    def _serialize_value(self, value: type) -> str:
        return serialize_class(value)

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        match deserialize_class(ser_value):
            case Err(e):
                return Err(e)
            case Ok(value):
                return self.set_value(value)

    @typechecked
    def get_value_field(self) -> Result[type[DataContainer], None]:
        match self.get_value():
            case Err(None):
                return Err(None)
            case Ok(v):
                value = v
        if value not in BUILTIN_TYPE_MAP.keys():
            return Err(None)
        return Ok(BUILTIN_TYPE_MAP[value])


ROS = TypeVar("ROS")


class RosContainer(DataContainer[ROS]):
    # This interface_kind has to be assigned a value in subclass definitions
    #   and should ONLY be a class attribute
    interface_kind: int
    interface_id: int

    @typechecked
    def __init__(
        self,
        interface_id=0,
        *args,
        **kwargs,
    ) -> None:
        if not hasattr(self, "interface_kind"):
            raise RuntimeError(
                "All concrete implementations have to specify a kind of ROS interface"
            )

        super().__init__(*args, **kwargs)

        self.interface_id = interface_id

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        config["interface_id"] = msg.interface_id
        return Ok(config)

    @classmethod
    @typechecked
    def from_msg(cls, msg: NodeDataType) -> Result[Self, str]:
        if not hasattr(cls, "interface_kind"):
            raise NotImplementedError("Called on abstract base class")
        if msg.ros_interface_kind != cls.interface_kind:
            return Err("Wrong kind of ROS interface")
        return super().from_msg(msg)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        return self.interface_id != other.interface_id

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.ros_interface_kind = self.interface_kind
        type_msg.interface_id = self.interface_id
        return type_msg


class RosNameContainer(RosContainer[str], BuiltinContainer[str]):
    type_identifier = NodeDataType.ROS_INTERFACE_NAME
    _value = "/foo"


@register_io_type
class RosTopicName(RosNameContainer):
    """
    Holds a ROS topic name as a string.
    """

    interface_kind = NodeDataType.ROS_TOPIC


@register_io_type
class RosServiceName(RosNameContainer):
    """
    Holds a ROS service name as a string.
    """

    interface_kind = NodeDataType.ROS_SERVICE


@register_io_type
class RosActionName(RosNameContainer):
    """
    Holds a ROS action name as a string.
    """

    interface_kind = NodeDataType.ROS_ACTION


class RosMsgContainer(RosContainer[Any]):
    """
    This is primarily used as a superclass for the `value_field`
    for RosTopicType and should probably not be used directly,
    as it requires specifying a message type as a class attribute.
    """

    type_identifier = NodeDataType.ROS_INTERFACE_VALUE
    interface_kind = NodeDataType.ROS_TOPIC

    # This message_type is set by the factory function
    message_type: type

    def __init__(self, *args, **kwargs) -> None:
        if not hasattr(self, "message_type"):
            raise RuntimeError(
                "All conctrete implementations have to specify a message_type. "
                "Use the factory function."
            )
        super().__init__(*args, **kwargs)

    def set_value(self, value: Any) -> Result[None, str]:
        if not isinstance(value, self.message_type):
            return Err(
                f"Value {value} is not the proper ROS message {self.message_type}"
            )
        return super().set_value(value)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        # This is basically an == check, since ROS types don't have inheritance.
        #   but we still use `issubclass` since it is a sufficient condition.
        return issubclass(other.message_type, self.message_type)

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.ros_msg_type = get_interface_name(self.message_type)
        return type_msg

    def _serialize_value(self, value: Any) -> str:
        return json.dumps(rosidl_runtime_py.message_to_ordereddict(value))

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        try:
            value = rosidl_runtime_py.set_message_fields(
                self.message_type,
                json.loads(ser_value),
            )
        except (TypeError, AttributeError, ValueError):
            return Err(
                f"Error populating message of type {self.message_type} with value {ser_value}"
            )
        return self.set_value(value)


@typechecked
def get_ros_msg_type(msg_type: type) -> Result[type[RosMsgContainer], str]:
    """
    Get an IO type class for a given ROS message type.
    """
    if not rosidl_runtime_py.utilities.is_message(msg_type):
        return Err(f"Type {msg_type} is not a valid message type")
    return Ok(
        type(
            f"{msg_type.__name__}Container",
            (RosMsgContainer,),
            {"message_type": msg_type},
        )
    )


class RosTypeContainer(RosContainer[type]):
    type_identifier = NodeDataType.ROS_INTERFACE_TYPE

    # Adapt defaults to common use case of type specification (static only)
    def __init__(
        self,
        allow_dynamic=False,
        allow_static=True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            *args,
            **kwargs,
        )

    @staticmethod
    @abc.abstractmethod
    def _validate(value: type) -> bool:
        raise NotImplementedError("Can't validate on a base class")

    @typechecked
    def set_value(self, value: type) -> Result[None, str]:
        if not self._validate(value):
            return Err(f"Value {value} is not a matching ROS type")
        return super().set_value(value)

    @typechecked
    def _serialize_value(self, value: type) -> str:
        return get_interface_name(value)

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        try:
            msg = rosidl_runtime_py.utilities.get_interface(ser_value)
        except ValueError:
            return Err(
                f"Serialized value {ser_value} does not point to a valid ROS interface"
            )
        return self.set_value(msg)


@register_io_type
class RosTopicType(TypeContainerMixin, RosTypeContainer):
    """
    This type holds message types of ROS topics.

    Note that this validation also accepts component messages like `_Request` or `_Goal`,
    since they're fully fledged message classes.
    The interface type just indicates that we are not looking for those.
    """

    interface_kind = NodeDataType.ROS_TOPIC
    _value = msg.Empty

    @staticmethod
    @typechecked
    def _validate(value: type) -> bool:
        return rosidl_runtime_py.utilities.is_message(value)

    @typechecked
    def get_value_field(self) -> Result[type[DataContainer], None]:
        match self.get_value():
            case Err(None):
                return Err(None)
            case Ok(v):
                value = v
        match get_ros_msg_type(value):
            case Err(e):
                raise RuntimeError(f"Cannot identify message type {value} due to {e}")
            case Ok(t):
                return Ok(t)


@register_io_type
class RosServiceType(RosTypeContainer):
    """
    This type holds message types of ROS services.
    """

    interface_kind = NodeDataType.ROS_SERVICE
    _value = srv.Trigger

    @staticmethod
    @typechecked
    def _validate(value: type) -> bool:
        return rosidl_runtime_py.utilities.is_service(value)


@register_io_type
class RosActionType(RosTypeContainer):
    """
    This type holds message types of ROS actions.
    """

    interface_kind = NodeDataType.ROS_ACTION
    _value = action.Fibonacci

    @staticmethod
    @typechecked
    def _validate(value: type) -> bool:
        return rosidl_runtime_py.utilities.is_action(value)


@register_io_type
class RosComponentType(RosTypeContainer):
    """
    This type holds component message types.

    This behaves similar to `RosTopicType`,
    except that this is not a valid `ReferenceType` target.
    The interface type indicates that we also want to allow
    interface components like `Service_Request` or `Action_Goal`.
    """

    interface_kind = NodeDataType.ROS_COMPONENT
    _value = msg.Empty

    @staticmethod
    @typechecked
    def _validate(value: type) -> bool:
        return rosidl_runtime_py.utilities.is_message(value)


class BuiltinOrRosType(TypeContainerMixin, DataContainer[type]):
    """
    This acts as a placeholder for type fields that can be filled by both a
    `BuiltinType` or a `RosTopicType`.
    This is not a functional type, any provided values are discarded immediately.
    Note that this doesn't support the restrictions that can be placed on `BuiltinType`.
    It is expected that this is replaced with either of the above
    when a node is fully configured.
    """

    type_identifier = NodeDataType.BUILTIN_OR_ROS_TYPE

    @classmethod
    @typechecked
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        return Err("Placeholder cannot be constructed from message")

    @typechecked
    def set_value(self, value: type) -> Result[None, str]:
        return Err("Placeholder does not accept values")

    @typechecked
    def is_compatible(self, other: DataContainer) -> bool:
        if isinstance(other, BuiltinType):
            return True
        if isinstance(other, RosTopicType):
            return True
        return False

    def serialize_type(self) -> NodeDataType:
        return super().serialize_type()

    @typechecked
    def _serialize_value(self, value: type) -> str:
        return ""

    @typechecked
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        return Err("Placeholder does not accept values")

    @typechecked
    def get_value_field(self) -> Result[type[DataContainer], None]:
        return Err(None)


class ReferenceContainer(DataContainer[Any]):
    _value: NoneType = None
    _reference: str
    _container_map: dict[str, DataContainer[Any]] = {}
    _inner_type: Optional[DataContainer[Any]] = None

    @typechecked
    def __init__(
        self,
        reference: str,
        allow_dynamic: bool = True,
        allow_static: bool = True,
        is_static: bool | None = None,
    ) -> None:
        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            is_static=is_static,
            value=None,
        )
        self._reference = reference

    def _set_inner_type(self) -> Result[None, str]:
        if self._reference not in self._container_map:
            return Err(f"Given reference {self._reference} is invalid")
        ref_obj = self._container_map[self._reference]
        if not isinstance(ref_obj, TypeContainerMixin):
            return Err("Reference doesn't implement TypeMixin")
        match ref_obj.get_value_field():
            case Err(None):
                return Err("Target didn't yield a value field")
            case Ok(c):
                inner_cls = c
        self._inner_type = inner_cls(
            allow_dynamic=self.allow_dynamic,
            allow_static=self.allow_static,
            is_static=self.is_static,
        )
        return Ok(None)

    @typechecked
    def set_type_map(self, new_map: dict[str, DataContainer[Any]]) -> Result[None, str]:
        self._container_map = new_map
        return self._set_inner_type()

    @classmethod
    @typechecked
    def _dict_from_msg(cls, msg: NodeDataType) -> Result[dict, str]:
        match super()._dict_from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(c):
                config = c
        config.pop("value")
        config["reference"] = msg.reference_target
        return Ok(config)

    def is_compatible(self, other: DataContainer) -> TypeGuard[Self]:
        if not super().is_compatible(other):
            return False
        return self._reference == other._reference

    def serialize_type(self) -> NodeDataType:
        if self._inner_type is None:
            type_msg = super().serialize_type()
        else:
            type_msg = self._inner_type.serialize_type()
            type_msg.type_identifier = self.type_identifier
        type_msg.reference_target = self._reference
        return type_msg

    @typechecked
    def get_runtime_type(self) -> DataContainer:
        if self._inner_type is None:
            return self
        return self._inner_type

    def set_value(self, value: Any) -> Result[None, str]:
        if self._inner_type is None:
            return Err("Can't set value on reference without target")
        return self._inner_type.set_value(value)

    def get_value(self) -> Result[Any, None]:
        if self._inner_type is None:
            return Err(None)
        return self._inner_type.get_value()

    def reset_value(self):
        if self._inner_type is None:
            return None
        return self._inner_type.reset_value()

    def is_updated(self) -> bool:
        if self._inner_type is None:
            return False
        return self._inner_type.is_updated()

    def reset_updated(self) -> None:
        if self._inner_type is None:
            return None
        return self._inner_type.reset_updated()

    @typechecked
    def _serialize_value(self, value: None) -> str:
        if self._inner_type is None:
            return ""
        return self._inner_type.serialize_value()

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        if self._inner_type is None:
            # Accept and ignore all serialized values if no inner type is set.
            return Ok(None)
        return self._inner_type.deserialize_value(ser_value)


@register_io_type
class ReferenceType(ReferenceContainer):
    """
    This IO type holds a reference to another IO type that holds a
    basic type as its value.
    This type acts as an IO type for that basic type.
    """

    type_identifier = NodeDataType.REFERENCE_TYPE


class IterableReferenceContainer(ReferenceContainer):
    max_length: int = 2**64 - 1
    strict_length: bool = False

    @typechecked
    def __init__(
        self,
        reference: str,
        max_length: Optional[int] = None,
        strict_length: Optional[bool] = None,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: Optional[bool] = None,
    ) -> None:
        if max_length is not None:
            self.max_length = max_length
        if strict_length is not None:
            self.strict_length = strict_length

        super().__init__(
            reference=reference,
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            is_static=is_static,
        )


@register_io_type
class ReferenceListType(ReferenceContainer):
    """
    This IO type holds a reference to another IO type that holds a
    basic type as its value.
    This type acts as an IO type for a list of that basic type.
    """

    type_identifier = NodeDataType.REFERENCE_LIST_TYPE

    @typechecked
    def _set_inner_type(self) -> Result[None, str]:
        match super()._set_inner_type():
            case Err(e):
                return Err(e)
            case Ok(None):
                pass
        self._inner_type = ListType(element_type=self._inner_type)
        return Ok(None)


@register_io_type
class ReferenceDictType(ReferenceContainer):
    """
    This IO type holds a reference to another IO type that holds a
    basic type as its value.
    This type acts as an IO type for a dict with values of that basic type.
    """

    type_identifier = NodeDataType.REFERENCE_DICT_TYPE

    @typechecked
    def _set_inner_type(self) -> Result[None, str]:
        match super()._set_inner_type():
            case Err(e):
                return Err(e)
            case Ok(None):
                pass
        self._inner_type = DictType(element_type=self._inner_type)
        return Ok(None)
