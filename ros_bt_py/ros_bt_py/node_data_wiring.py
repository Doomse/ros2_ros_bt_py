# Copyright 2025 FZI Forschungszentrum Informatik
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
import uuid

from typeguard import typechecked

from ros_bt_py.ros_helpers import uuid_to_ros
from ros_bt_py_interfaces.msg import Wiring


@typechecked
class NodeDataWiring(object):

    def __init__(
        self,
        source_id: uuid.UUID,
        source_kind: str,
        source_key: str,
        target_id: uuid.UUID,
        target_kind: str,
        target_key: str,
    ):
        self.source_id = source_id
        self.source_kind = source_kind
        self.source_key = source_key
        self.target_id = target_id
        self.target_kind = target_kind
        self.target_key = target_key

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, NodeDataWiring):
            return False
        return (
            self.source_id == value.source_id
            and self.source_kind == value.source_kind
            and self.source_key == value.source_key
            and self.target_id == value.target_id
            and self.target_kind == value.target_kind
            and self.target_key == value.target_key
        )

    def has_same_target(self, value: "NodeDataWiring") -> bool:
        return (
            self.target_id == value.target_id
            and self.target_kind == value.target_kind
            and self.target_key == value.target_key
        )

    def to_wiring_msg(self) -> Wiring:
        wiring = Wiring()
        wiring.source.node_id = uuid_to_ros(self.source_id)
        wiring.source.data_kind = self.source_kind
        wiring.source.data_key = self.source_key
        wiring.target.node_id = uuid_to_ros(self.target_id)
        wiring.target.data_kind = self.target_kind
        wiring.target.data_key = self.target_key
        return wiring
