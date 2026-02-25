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
import json
from typing import Any, Generic, Optional, Self, Type, TypeGuard, TypeVar
from typeguard import TypeCheckError, check_type, typechecked

from ros_bt_py.vendor.result import Result, Ok, Err

from ros_bt_py_interfaces.msg import NodeDataType


class DataContainer(abc.ABC):

    type_identifier: int
    allow_dynamic: bool
    allow_static: bool
    is_static: bool
    _value: Any
    _updated: bool

    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
        value: Any = None,
    ) -> None:
        """
        This method calls `set_value` with the given value,
            so subclasses need to insure that method is functional
            before calling `super().__init__`.
        This also raises `ValueError` if the given value can't be assigned
        """
        super().__init__()

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
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        """
        Subclasses should extend this to add their specific configs
        """
        return {
            "allow_dynamic": msg.allow_dynamic,
            "allow_static": msg.allow_static,
            "is_static": msg.is_static,
            "value": None,
        }

    @classmethod
    def from_msg(cls, msg: NodeDataType) -> Result["DataContainer", str]:
        if not hasattr(cls, "type_identifier"):
            raise NotImplementedError("Called on abstract base class")
        if msg.type_identifier != cls.type_identifier:
            return Err("Wrong type identifier")
        return Ok(cls(**cls._dict_from_msg(msg)))

    @abc.abstractmethod
    def set_value(self, value: Any) -> Result[None, str]:
        """
        Subclasses should validate and clean incoming values
            before calling `super().set_value` to assign them.
        """
        if self.is_static and self._value is not None:
            return Err("Static value was already asssigned")
        self._value = value
        self._updated = True
        return Ok(None)

    def get_value(self) -> Result[Any, None]:
        """
        Subclasses should wrap this to include proper type constraints.
        """
        if self._value is None:
            return Err(None)
        return Ok(self._value)

    def is_updated(self):
        return self._updated

    def reset_updated(self):
        self._updated = False

    @abc.abstractmethod
    def is_compatible(self, other: "DataContainer") -> bool:
        """
        Check if the given container is compatible with this one,
            meaning that its constraints are at least as narrow as the ones in `self`.
        This is used to compare configs given on specific nodes
            with the baseline given on the class config.

        Note that the default implementation does NOT
            consider the container class hierarchy !!!

        Subclasses should extend this with additional checks.
        """
        compatible = True
        if self.allow_dynamic and not other.allow_dynamic:
            compatible = False
        if self.allow_static and not other.allow_static:
            compatible = False
        # Don't compare `is_static`, since it serves as a default value not a constraint
        return compatible

    @abc.abstractmethod
    def serialize_type(self) -> NodeDataType:
        return NodeDataType(
            type_identifier=self.type_identifier,
            allow_dynamic=self.allow_dynamic,
            allow_static=self.allow_static,
            is_static=self.is_static,
        )

    @abc.abstractmethod
    def serialize_value(self) -> str:
        raise NotImplementedError("Can't serialize a base class value")

    @abc.abstractmethod
    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        raise NotImplementedError("Can't deserialize a base class value")


BUILTIN = TypeVar("BUILTIN", bool, int, float, str, list, dict, bytes)


@typechecked
class BuiltinContainer(DataContainer, Generic[BUILTIN]):

    _type: Type[BUILTIN]
    _value: Optional[BUILTIN]

    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
        value: Optional[BUILTIN] = None,
    ) -> None:
        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            is_static=is_static,
            value=value,
        )

    def set_value(self, value: BUILTIN) -> Result[None, str]:
        if not isinstance(value, self._type):
            return Err(f"Given value {value} isn't of type {self._type}")
        return super().set_value(value)

    def get_value(self) -> Result[BUILTIN, None]:
        return super().get_value()

    def is_compatible(self, other: DataContainer) -> TypeGuard[Type[Self]]:
        if not isinstance(other, self.__class__):
            return False
        return super().is_compatible(other)

    def serialize_value(self) -> str:
        match self.get_value():
            case Err(None):
                return ""
            case Ok(v):
                return json.dumps(v)

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        value = json.loads(ser_value)
        return self.set_value(value)


@typechecked
class BoolContainer(BuiltinContainer[bool]):

    type_identifier = NodeDataType.BOOL_TYPE
    _type = bool
    _value = False

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        # Nothing to add for bool
        return super()._dict_from_msg(msg)

    def serialize_type(self) -> NodeDataType:
        # Nothing to add for bool
        return super().serialize_type()


NUM = TypeVar("NUM", int, float)


@typechecked
class NumericContainer(BuiltinContainer[NUM]):

    min_value: NUM
    max_value: NUM
    lower_limit: NUM
    upper_limit: NUM

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

    def set_value(self, value: NUM) -> Result[None, str]:
        if value < self.min_value:
            return Err(f"Given value {value} is smaller than minimum {self.min_value}")
        if value > self.max_value:
            return Err(f"Given value {value} is larger than maximum {self.max_value}")
        return super().set_value(value)

    def is_compatible(self, other: DataContainer) -> bool:
        if not super().is_compatible(other):
            return False
        if other.min_value < self.min_value:
            return False
        if other.max_value > self.max_value:
            return False
        return True


@typechecked
class IntContainer(NumericContainer[int]):

    type_identifier = NodeDataType.INT_TYPE
    _type = int
    _value = 0
    lower_limit = -(2**63)
    upper_limit = 2**63 - 1

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        config["min_value"] = msg.int_min_value
        config["max_value"] = msg.int_max_value
        return config

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.int_min_value = self.min_value
        type_msg.int_max_value = self.max_value
        return type_msg


@typechecked
class FloatContainer(NumericContainer[float]):

    type_identifier = NodeDataType.FLOAT_TYPE
    _type = float
    lower_limit = -1.7976931348623158e308
    upper_limit = 1.7976931348623158e308

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        config["min_value"] = msg.float_min_value
        config["max_value"] = msg.float_max_value
        return config

    def set_value(self, value: float) -> Result[None, str]:
        # Silently convert int to float
        if isinstance(value, int):
            value = float(value)
        return super().set_value(value)

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.float_min_value = self.min_value
        type_msg.float_max_value = self.max_value
        return type_msg


ITER = TypeVar("ITER", str, list, dict, bytes)


@typechecked
class IterableContainer(BuiltinContainer[ITER]):

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
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        config["max_length"] = msg.max_length
        config["strict_length"] = msg.strict_length
        return config

    def set_value(self, value: ITER) -> Result[None, str]:
        if len(value) > self.max_length:
            return Err(
                f"Value {value} has more items than the limit of {self.max_length}"
            )
        if self.strict_length and len(value) < self.max_length:
            return Err(
                f"Value {value} has fewer items than the limit of {self.max_length}"
            )
        # Since (some) iterables are mutable, we copy them on assignment
        return super().set_value(deepcopy(value))

    def get_value(self) -> Result[ITER, None]:
        match super().get_value():
            case Err(None):
                return Err(None)
            case Ok(v):
                # Since (some) iterables are mutable, we copy them on fetch
                return Ok(deepcopy(v))

    def is_compatible(self, other: DataContainer) -> bool:
        if not super().is_compatible(other):
            return False
        if other.max_length > self.max_length:
            return False
        if not other.strict_length and self.strict_length:
            return False
        return True

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.max_length = self.max_length
        type_msg.strict_length = self.strict_length
        return type_msg


class StringContainer(IterableContainer[str]):
    type_identifier = NodeDataType.STRING_TYPE
    _type = str
    _value = ""


class ListContainer(IterableContainer[list]):
    type_identifier = NodeDataType.LIST_TYPE
    _type = list
    _value = []


class DictContainer(IterableContainer[dict]):
    type_identifier = NodeDataType.DICT_TYPE
    _type = dict
    _value = {}


class BytesContainer(IterableContainer[bytes]):
    type_identifier = NodeDataType.BYTES_TYPE
    _type = bytes
    _value = b"\x00"

    # The max_length default is set to one (strict), since bytes are mostly used
    #   to fill byte fields in ROS messages, which only take one byte.
    max_length = 1
    strict_length = True

    def serialize_value(self) -> str:
        match self.get_value():
            case Err(None):
                return ""
            case Ok(v):
                return v.hex(" ")

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        try:
            value = bytes.fromhex(ser_value)
        except ValueError:
            return Err(f"Given string {ser_value} isn't valid hexcode")
        return self.set_value(value)
