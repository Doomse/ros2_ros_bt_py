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
import pytest

import rclpy
import time

from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)

from example_interfaces.msg import String

from ros_bt_py_interfaces.srv import ControlTreeExecution

from tests.integration.conftest import TreeControlNode, launch_description


@pytest.mark.launch(fixture=launch_description)
def test_topic_publisher_node(tree_control_node: TreeControlNode):
    load_result = tree_control_node.load_tree(
        "trees/ros_nodes_isolation/topic_publish.yaml"
    )
    assert load_result.is_ok()

    has_received_msg = False

    def _recieve_msg(msg: String):
        nonlocal has_received_msg
        if msg.data == "foobarbaz":
            has_received_msg = True

    msg_subscriber = tree_control_node.create_subscription(
        String,
        "/foo",
        _recieve_msg,
        QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            depth=1,
        ),
    )

    has_received_msg = False
    run_result = tree_control_node.execute_tree(ControlTreeExecution.Request.TICK_ONCE)
    assert run_result.is_ok()

    # We expect a message after the first tick
    start_time = time.time()
    while start_time + 30 > time.time():
        if has_received_msg:
            break
        assert rclpy.ok()
        rclpy.spin_once(tree_control_node, timeout_sec=5)
    assert has_received_msg

    has_received_msg = False
    run_result = tree_control_node.execute_tree(ControlTreeExecution.Request.TICK_ONCE)
    assert run_result.is_ok()

    # We expect NO message after the second tick
    start_time = time.time()
    while start_time + 30 > time.time():
        if has_received_msg:
            break
        assert rclpy.ok()
        rclpy.spin_once(tree_control_node, timeout_sec=5)
    assert not has_received_msg

    has_received_msg = False
    run_result = tree_control_node.execute_tree(ControlTreeExecution.Request.RESET)
    assert run_result.is_ok()
    run_result = tree_control_node.execute_tree(ControlTreeExecution.Request.TICK_ONCE)
    assert run_result.is_ok()

    # We expect a message after a reset
    start_time = time.time()
    while start_time + 30 > time.time():
        if has_received_msg:
            break
        assert rclpy.ok()
        rclpy.spin_once(tree_control_node, timeout_sec=5)
    assert has_received_msg

    tree_control_node.destroy_subscription(msg_subscriber)
