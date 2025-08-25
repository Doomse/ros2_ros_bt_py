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

from result import Result, Ok, Err
from ros_bt_py.node import Leaf, Decorator, define_bt_node
from ros_bt_py.node_config import NodeConfig, OptionRef
from ros_bt_py.helpers import TickReturnState, UntickReturnState
from ros_bt_py.exceptions import BehaviorTreeException


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        options={},
        inputs={"list": list},
        outputs={"length": int},
        max_children=0,
    )
)
class ListLength(Leaf):
    """Compute list length."""

    def _do_setup(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_tick(self) -> Result[TickReturnState, BehaviorTreeException]:
        self.outputs["length"] = len(self.inputs["list"])
        return Ok(TickReturnState.SUCCEEDED)

    def _do_shutdown(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_reset(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_untick(self) -> Result[UntickReturnState, BehaviorTreeException]:
        return Ok(UntickReturnState.IDLE)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        options={"element_type": type, "index": int},
        inputs={"list": list},
        outputs={"element": OptionRef("element_type")},
        max_children=0,
    )
)
class GetListElementOption(Leaf):
    """Return element at given index in the list."""

    def _do_setup(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_tick(self) -> Result[TickReturnState, BehaviorTreeException]:
        self.outputs["element"] = self.inputs["list"][self.options["index"]]
        return Ok(TickReturnState.SUCCEEDED)

    def _do_shutdown(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_reset(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_untick(self) -> Result[UntickReturnState, BehaviorTreeException]:
        return Ok(UntickReturnState.IDLE)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        options={"element_type": type, "index": int},
        inputs={"list": list, "element": OptionRef("element_type")},
        outputs={"list": list},
        max_children=0,
    )
)
class InsertInList(Leaf):
    """Return a new list with the inserted element."""

    def _do_setup(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_tick(self) -> Result[TickReturnState, BehaviorTreeException]:
        if self.inputs.is_updated("list") or self.inputs.is_updated("element"):
            self.outputs["list"] = list(self.inputs["list"])
            self.outputs["list"].insert(self.options["index"], self.inputs["element"])
        return Ok(TickReturnState.SUCCEEDED)

    def _do_shutdown(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_reset(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)

    def _do_untick(self) -> Result[UntickReturnState, BehaviorTreeException]:
        return Ok(UntickReturnState.IDLE)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        options={"compare_type": type, "list": list},
        inputs={"in": OptionRef("compare_type")},
        outputs={},
        max_children=0,
    )
)
class IsInList(Leaf):
    """
    Check if `in` is in provided list.

    Will succeed if `in` is in `list` and fail otherwise
    """

    def _do_setup(self) -> Result[None, BehaviorTreeException]:
        self._received_in = False
        return Ok(None)

    def _do_tick(self) -> Result[TickReturnState, BehaviorTreeException]:
        if not self._received_in:
            if self.inputs.is_updated("in"):
                self._received_in = True

        if self._received_in and self.inputs["in"] in self.options["list"]:
            return Ok(TickReturnState.SUCCEEDED)
        else:
            return Ok(TickReturnState.FAILED)

    def _do_untick(self) -> Result[UntickReturnState, BehaviorTreeException]:
        # Nothing to do
        return Ok(UntickReturnState.IDLE)

    def _do_reset(self) -> Result[None, BehaviorTreeException]:
        self.inputs.reset_updated()
        self._received_in = False

        return Ok(None)

    def _do_shutdown(self) -> Result[None, BehaviorTreeException]:
        # Nothing to do
        return Ok(None)


@define_bt_node(
    NodeConfig(
        version="0.1.0",
        options={"item_type": type},
        inputs={"list": list},
        outputs={"list_item": OptionRef("item_type")},
        max_children=1,
    )
)
class IterateList(Decorator):
    """
    Iterate through list provided as input.

    The elements in the list are iterated on the output list_item.
    The iteration goes when the decorated child returns a success.
    This node returns running until the iteration is done.
    If it managed to iterate through the list, it returns success.
    If the decorated child returned failure, it fails.
    """

    def _do_setup(self) -> Result[None, BehaviorTreeException]:
        self.reset_counter()
        if len(self.children) == 1:
            result = self.children[0].setup()
            return result
        return Ok(None)

    def reset_counter(self):
        self.output_changed = True
        self.counter = 0

    def _do_tick(self) -> Result[TickReturnState, BehaviorTreeException]:
        if self.inputs.is_updated("list"):
            self.logdebug("Input list changed - resetting iterator")
            self.reset_counter()

        # if no items in 'list' directly succeed
        if len(self.inputs["list"]) > 0:
            self.outputs["list_item"] = self.inputs["list"][self.counter]
        else:
            self.logdebug("Nothing to iterate, input list is empty")
            return Ok(TickReturnState.SUCCEEDED)

        if len(self.children) == 0:
            self.counter += 1
            if self.counter == len(self.inputs["list"]):
                self.reset_counter()
                return Ok(TickReturnState.SUCCEEDED)
        else:
            if self.output_changed:
                # let one tick go for the tree to digest our new output before childs are ticked
                self.output_changed = False
                return Ok(TickReturnState.RUNNING)
            for child in self.children:
                result = child.tick()
                if result.is_err():
                    return result
                if result.ok() == TickReturnState.SUCCEEDED:
                    # we only increment the counter when the child succeeded
                    self.counter += 1
                    self.output_changed = True
                    if self.counter == len(self.inputs["list"]):
                        self.reset_counter()
                        return Ok(TickReturnState.SUCCEEDED)
                elif result.ok() == TickReturnState.FAILED:
                    # child failed: we failed
                    return Ok(TickReturnState.FAILED)
        return Ok(TickReturnState.RUNNING)

    def _do_untick(self) -> Result[UntickReturnState, BehaviorTreeException]:
        for child in self.children:
            return child.untick()
        return Ok(UntickReturnState.IDLE)

    def _do_reset(self) -> Result[None, BehaviorTreeException]:
        self.inputs.reset_updated()
        self.reset_counter()
        for child in self.children:
            return child.reset()
        return Ok(None)

    def _do_shutdown(self) -> Result[None, BehaviorTreeException]:
        return Ok(None)
