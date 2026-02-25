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
from types import NoneType
from typing import Any, Generic, Optional, Self, Type, TypeGuard, TypeVar
from typeguard import typechecked

from ros_bt_py.vendor.result import Result, Ok, Err

from ros_bt_py_interfaces.msg import NodeDataType


ANY = TypeVar("ANY")


class DataContainer(abc.ABC, Generic[ANY]):

    type_identifier: int
    allow_dynamic: bool
    allow_static: bool
    is_static: bool
    _value: ANY
    _updated: bool

    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
        value: Optional[ANY] = None,
    ) -> None:
        """
        This method calls `set_value` with the given value,
            so subclasses need to ensure that method is functional
            before calling `super().__init__`.
        This also raises `ValueError` if the given value can't be assigned
        """
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
    def from_msg(cls, msg: NodeDataType) -> Result[Self, str]:
        if not hasattr(cls, "type_identifier"):
            raise NotImplementedError("Called on abstract base class")
        if msg.type_identifier != cls.type_identifier:
            return Err("Wrong type identifier")
        obj = cls(**cls._dict_from_msg(msg))
        match obj.deserialize_value(msg.serialized_value):
            case Err(e):
                return Err(e)
            case Ok(None):
                pass
        return Ok(obj)

    @abc.abstractmethod
    def set_value(self, value: ANY) -> Result[None, str]:
        """
        Subclasses should validate and clean incoming values
            before calling `super().set_value` to assign them.
        """
        if self.is_static and self._value is not None:
            return Err("Static value was already asssigned")
        self._value = value
        self._updated = True
        return Ok(None)

    def get_value(self) -> Result[ANY, None]:
        """
        Subclasses should wrap this to include proper type constraints.
        """
        if self._value is None:
            return Err(None)
        return Ok(self._value)

    def is_updated(self) -> bool:
        return self._updated

    def reset_updated(self) -> None:
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
            serialized_value=self.serialize_value(),
        )

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
        raise NotImplementedError("Can't deserialize a base class value")


class TypeContainerMixin(abc.ABC):

    @abc.abstractmethod
    def get_value_field(self) -> Result[Type[DataContainer], None]:
        raise NotImplementedError("Can't get value field for base class")


BUILTIN = TypeVar("BUILTIN", bool, int, float, str, list, dict, bytes)


@typechecked
class BuiltinContainer(DataContainer[BUILTIN]):

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

    def _serialize_value(self, value: BUILTIN) -> str:
        return json.dumps(obj=value)

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        value = json.loads(ser_value)
        return self.set_value(value)


@typechecked
class BoolType(BuiltinContainer[bool]):

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
class IntType(NumericContainer[int]):

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
class FloatType(NumericContainer[float]):

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


class StringType(IterableContainer[str]):
    type_identifier = NodeDataType.STRING_TYPE
    _type = str
    _value = ""


class ListType(IterableContainer[list]):
    type_identifier = NodeDataType.LIST_TYPE
    _type = list
    _value = []


class DictType(IterableContainer[dict]):
    type_identifier = NodeDataType.DICT_TYPE
    _type = dict
    _value = {}


class BytesType(IterableContainer[bytes]):
    type_identifier = NodeDataType.BYTES_TYPE
    _type = bytes
    _value = b"\x00"

    # The max_length default is set to one (strict), since bytes are mostly used
    #   to fill byte fields in ROS messages, which only take one byte.
    max_length = 1
    strict_length = True

    def _serialize_value(self, value: bytes) -> str:
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


BUILTIN_TYPE_MAP = {
    bool: BoolType,
    int: IntType,
    float: FloatType,
    str: StringType,
    list: ListType,
    dict: DictType,
    bytes: BytesType,
}


def serialize_class(cls: type) -> str:
    type_name = cls.__name__
    module = getmodule(cls)
    module_name = module.__name__ + "." if module is not None else ""
    return module_name + type_name


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


class BuiltinType(TypeContainerMixin, DataContainer[type]):

    type_identifier = NodeDataType.BUILTIN_TYPE
    _value = int

    def __init__(self, valid_types=BUILTIN_TYPE_MAP.keys(), *args, **kwargs) -> None:
        self.valid_types = valid_types
        super().__init__(*args, **kwargs)

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        valid_types: list[type] = []
        for option in msg.value_options:
            match deserialize_class(option):
                case Err(_):
                    continue
                case Ok(c):
                    valid_types.append(c)
        config["valid_types"] = valid_types
        return config

    def set_value(self, value: type) -> Result[None, str]:
        if value not in self.valid_types:
            return Err(
                f"Type {value} is a valid type. Valid types: {list(self.valid_types)}"
            )
        return super().set_value(value)

    def is_compatible(self, other: DataContainer) -> bool:
        if not isinstance(other, self.__class__):
            return False
        for type_elem in other.valid_types:
            if type_elem not in self.valid_types:
                return False
        return super().is_compatible(other)

    def serialize_type(self) -> NodeDataType:
        type_msg = super().serialize_type()
        type_msg.value_options = []
        for type_elem in self.valid_types:
            type_msg.value_options.append(serialize_class(type_elem))
        return type_msg

    def _serialize_value(self, value: type) -> str:
        return serialize_class(value)

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        match deserialize_class(ser_value):
            case Err(e):
                return Err(e)
            case Ok(value):
                return self.set_value(value)

    def get_value_field(self) -> Result[Type[DataContainer], None]:
        match self.get_value():
            case Err(None):
                return Err(None)
            case Ok(v):
                value = v
        if value not in BUILTIN_TYPE_MAP.keys():
            return Err(None)
        return Ok(BUILTIN_TYPE_MAP[value])


class ReferenceType(DataContainer[Any]):

    type_identifier = NodeDataType.REFERENCE_TYPE
    _value: NoneType = None
    _reference: str
    _container_map: dict[str, DataContainer[Any]] = {}
    _inner_type: Optional[DataContainer[Any]] = None

    def __init__(
        self,
        reference: str,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
    ) -> None:
        super().__init__(allow_dynamic, allow_static, is_static, None)
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

    def set_type_map(self, new_map: dict[str, DataContainer[Any]]):
        self._container_map = new_map
        # Ignore errors on setting inner type, it does not HAVE to be set at this point
        self._set_inner_type()

    def inner_from_msg(self, msg: NodeDataType) -> Result[None, str]:
        match self._set_inner_type():
            case Err(e):
                return Err(e)
            case Ok(None):
                pass
        assert (
            self._inner_type is not None
        ), "We just assigned an inner type without issues"
        inner_cls = self._inner_type.__class__
        msg_copy = deepcopy(msg)
        msg_copy.type_identifier = inner_cls.type_identifier
        match inner_cls.from_msg(msg):
            case Err(e):
                return Err(e)
            case Ok(t):
                self._inner_type = t
                return Ok(None)

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        config.pop("value")
        config["reference"] = msg.reference_target
        return config

    def set_value(self, value: Any) -> Result[None, str]:
        if self._inner_type is None:
            return Err("Can't set value on reference without target")
        return self._inner_type.set_value(value)

    def get_value(self) -> Result[Any, None]:
        if self._inner_type is None:
            return Err(None)
        return self._inner_type.get_value()

    def is_updated(self) -> bool:
        if self._inner_type is None:
            return False
        return self._inner_type.is_updated()

    def reset_updated(self) -> None:
        if self._inner_type is None:
            return None
        return self._inner_type.reset_updated()

    def is_compatible(self, other: DataContainer) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._reference == other._reference

    def serialize_type(self) -> NodeDataType:
        if self._inner_type is None:
            type_msg = super().serialize_type()
        else:
            type_msg = self._inner_type.serialize_type()
        type_msg.reference_target = self._reference
        return type_msg

    def _serialize_value(self, value: None) -> str:
        if self._inner_type is None:
            return ""
        return self._inner_type.serialize_value()

    def deserialize_value(self, ser_value: str) -> Result[None, str]:
        if self._inner_type is None:
            # Accept and ignore all serialized values if no inner type is set.
            return Ok(None)
        return self._inner_type.deserialize_value(ser_value)
