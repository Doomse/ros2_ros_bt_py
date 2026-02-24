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
from typing import Any, Optional
from typeguard import typechecked

from ros_bt_py.vendor.result import Result, Ok, Err

from ros_bt_py_interfaces.msg import NodeDataType


@typechecked
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

        if not self.allow_dynamic and not self.allow_static:
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
        match self.set_value(value):
            case Err(e):
                raise ValueError(e)
            case Ok(None):
                pass

    @abc.abstractmethod
    @classmethod
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
    def get_serialized_type(self) -> NodeDataType:
        return NodeDataType(
            type_identifier=self.type_identifier,
            allow_dynamic=self.allow_dynamic,
            allow_static=self.allow_static,
            is_static=self.is_static,
        )

    @abc.abstractmethod
    def get_serialized_value(self) -> str:
        raise NotImplementedError("Can't serialize a base class")


class BoolContainer(DataContainer):

    type_identifier = NodeDataType.BOOL_TYPE
    _value: Optional[bool]

    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
        value: Optional[bool] = None,
    ) -> None:
        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            is_static=is_static,
            value=value,
        )

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        return config

    def set_value(self, value: bool) -> Result[None, str]:
        # Nothing to do for bool values
        return super().set_value(value)

    def get_value(self) -> Result[bool, None]:
        return super().get_value()

    def is_compatible(self, other: DataContainer) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return super().is_compatible(other)

    def get_serialized_type(self) -> NodeDataType:
        type_msg = super().get_serialized_type()
        # Nothing to do for bool values
        return type_msg

    def get_serialized_value(self) -> str:
        match self.get_value():
            case Err(None):
                return ""
            case Ok(b):
                return str(b).lower()


class IntContainer(DataContainer):

    type_identifier = NodeDataType.INT_TYPE
    _value: Optional[int]
    min_value: Optional[int]
    max_value: Optional[int]

    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
        value: Optional[int] = None,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
    ) -> None:
        self.min_value = min_value
        self.max_value = max_value

        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            is_static=is_static,
            value=value,
        )

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        config["min_value"] = msg.int_min_value
        config["max_value"] = msg.int_max_value
        return config

    def set_value(self, value: int) -> Result[None, str]:
        if self.min_value is not None:
            if value < self.min_value:
                return Err(
                    f"Given value {value} is smaller than minimum {self.min_value}"
                )
        if self.max_value is not None:
            if value > self.max_value:
                return Err(
                    f"Given value {value} is larger than maximum {self.max_value}"
                )
        return super().set_value(value)

    def get_value(self) -> Result[int, None]:
        return super().get_value()

    def is_compatible(self, other: DataContainer) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.min_value is not None:
            if other.min_value is None or other.min_value < self.min_value:
                return False
        if self.max_value is not None:
            if other.max_value is None or other.max_value > self.max_value:
                return False
        return super().is_compatible(other)

    def get_serialized_type(self) -> NodeDataType:
        type_msg = super().get_serialized_type()
        type_msg.int_min_value = (
            self.min_value if self.min_value is not None else -(2**63)
        )
        type_msg.int_max_value = (
            self.max_value if self.max_value is not None else 2**63 - 1
        )
        return type_msg

    def get_serialized_value(self) -> str:
        match self.get_value():
            case Err(None):
                return ""
            case Ok(i):
                return str(i)


class FloatContainer(DataContainer):

    type_identifier = NodeDataType.FLOAT_TYPE
    _value: Optional[float]
    min_value: Optional[float]
    max_value: Optional[float]

    def __init__(
        self,
        allow_dynamic: bool = True,
        allow_static: bool = False,
        is_static: bool | None = None,
        value: Optional[float] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> None:
        self.min_value = min_value
        self.max_value = max_value

        super().__init__(
            allow_dynamic=allow_dynamic,
            allow_static=allow_static,
            is_static=is_static,
            value=value,
        )

    @classmethod
    def _dict_from_msg(cls, msg: NodeDataType) -> dict:
        config = super()._dict_from_msg(msg)
        config["min_value"] = msg.float_min_value
        config["max_value"] = msg.float_max_value
        return config

    def set_value(self, value: float | int) -> Result[None, str]:
        float_val = float(value)
        if self.min_value is not None:
            if float_val < self.min_value:
                return Err(
                    f"Given value {float_val} is smaller than minimum {self.min_value}"
                )
        if self.max_value is not None:
            if float_val > self.max_value:
                return Err(
                    f"Given value {float_val} is larger than maximum {self.max_value}"
                )
        return super().set_value(float_val)

    def get_value(self) -> Result[float, None]:
        return super().get_value()

    def is_compatible(self, other: DataContainer) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.min_value is not None:
            if other.min_value is None or other.min_value < self.min_value:
                return False
        if self.max_value is not None:
            if other.max_value is None or other.max_value > self.max_value:
                return False
        return super().is_compatible(other)

    def get_serialized_type(self) -> NodeDataType:
        type_msg = super().get_serialized_type()
        type_msg.int_min_value = (
            self.min_value if self.min_value is not None else -1.7976931348623158e308
        )
        type_msg.int_max_value = (
            self.max_value if self.max_value is not None else 1.7976931348623158e308
        )
        return type_msg

    def get_serialized_value(self) -> str:
        match self.get_value():
            case Err(None):
                return ""
            case Ok(f):
                return str(f)
