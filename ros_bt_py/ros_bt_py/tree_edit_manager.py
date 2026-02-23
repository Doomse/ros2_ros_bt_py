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
import inspect
import uuid
from copy import deepcopy
from typing import Any, Dict, Optional, List, cast

from ros_bt_py.tree_exec_manager import TreeExecManager, is_edit_service
from ros_bt_py.vendor.result import Err, Ok, Result

from typeguard import typechecked

from ros_bt_py_interfaces.srv import (
    AddNode,
    AddNodeAtIndex,
    ChangeTreeName,
    GenerateSubtree,
    GetSubtree,
    LoadTree,
    MorphNode,
    MoveNode,
    RemoveNode,
    ReplaceNode,
    SetOptions,
    WireNodeData,
)

from ros_bt_py.debug_manager import DebugManager
from ros_bt_py.exceptions import BehaviorTreeException
from ros_bt_py.helpers import (
    BTNodeState,
    json_decode,
)
from ros_bt_py.ros_helpers import ros_to_uuid, uuid_to_ros
from ros_bt_py.node_config import OptionRef

from rclpy_message_converter import message_converter


class TreeEditManager(TreeExecManager):
    """
    Provide methods to edit a Behavior Tree
      in addition to the inherited load and run functions.

    These methods are suited (intended, even) for use as ROS service handlers.
    """

    @typechecked
    def find_nodes_in_cycles(self) -> List[uuid.UUID]:
        """Return a list of all nodes in the tree that are part of cycles."""
        safe_node_ids: List[uuid.UUID] = []
        nodes_in_cycles: List[uuid.UUID] = []
        # Follow the chain of parent nodes for each node name in self.nodes
        for starting_id in self.nodes.keys():
            cycle_candidates = [starting_id]
            current_node = self.nodes[starting_id]
            while current_node.parent:
                current_node = self.nodes[current_node.parent.node_id]
                cycle_candidates.append(current_node.node_id)
                if current_node.node_id == starting_id:
                    nodes_in_cycles.extend(cycle_candidates)
                    break
                if current_node.node_id in safe_node_ids:
                    # We've already checked for cycles from the parent node, no
                    # need to do that again.
                    safe_node_ids.extend(cycle_candidates)
                    break
            if not current_node.parent:
                safe_node_ids.extend(cycle_candidates)

        return nodes_in_cycles

    ####################
    # Service Handlers #
    ####################

    @is_edit_service
    @typechecked
    def add_node(
        self, request: AddNode.Request, response: AddNode.Response
    ) -> AddNode.Response:
        """
        Add the node in this request to the tree.

        :param ros_bt_py_msgs.srv.AddNodeRequest request:
          A request describing the node to add.
        """
        internal_request = AddNodeAtIndex.Request(
            parent_node_id=request.parent_node_id,
            node=request.node,
            new_child_index=-1,
        )
        internal_response = AddNodeAtIndex.Response()
        internal_response = self.add_node_at_index(
            request=internal_request, response=internal_response
        )

        response.success = internal_response.success
        response.error_message = internal_response.error_message
        return response

    @is_edit_service
    @typechecked
    def add_node_at_index(
        self, request: AddNodeAtIndex.Request, response: AddNodeAtIndex.Response
    ) -> AddNodeAtIndex.Response:
        """
        Add the node in this request to the tree.

        The `node_id` from the request is discarded and a new one is randomly generated.
        The actual is that the node is assigned is included in the response.

        :param ros_bt_py_msgs.srv.AddNodeAtIndexRequest request:
            A request describing the node to add.
        """
        node_msg = request.node
        node_msg.node_id = uuid_to_ros(uuid.uuid4())
        instance_result = self.instantiate_node_from_msg(
            node_msg=request.node,
            ros_node=self.ros_node,
        )
        if instance_result.is_err():
            response.success = False
            response.error_message = str(instance_result.unwrap_err())
            return response
        instance = instance_result.unwrap()

        response.success = True
        response.node_id = node_msg.node_id

        match ros_to_uuid(request.parent_node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(p_id):
                parent_node_id = p_id

        # Add node as child of the named parent, if any
        if parent_node_id != uuid.UUID(int=0):
            if parent_node_id not in self.nodes:
                response.success = False
                response.error_message = (
                    f"Parent {request.parent_node_id} of node "
                    f"{instance.name} does not exist!"
                )
                # Remove node from tree
                self.remove_node(
                    request=RemoveNode.Request(
                        node_name=instance.name, remove_children=False
                    ),
                    response=RemoveNode.Response(),
                )
                return response

            add_child_result = self.nodes[parent_node_id].add_child(
                child=instance, at_index=request.new_child_index
            )
            if add_child_result.is_err():
                response.success = False
                response.error_message = str(add_child_result.unwrap_err())
                return response

        # Add children from msg to node
        missing_children = []
        for child_id in request.node.child_ids:
            if child_id in self.nodes:
                add_child_result = instance.add_child(self.nodes[child_id])
                if add_child_result.is_err():
                    self.get_logger().warn(
                        f"Could not add child: {str(add_child_result.unwrap_err())}"
                    )
                    missing_children.append(child_id)
            else:
                missing_children.append(child_id)
        if missing_children:
            response.success = False
            response.error_message = (
                f"Children for node {instance.name} are not or could not be added"
                f"in tree: {str(missing_children)}"
            )
            # Remove node from tree to restore state before insertion attempt
            self.remove_node(
                request=RemoveNode.Request(
                    node_name=instance.name, remove_children=False
                ),
                response=RemoveNode.Response(),
            )

        nodes_in_cycles = self.find_nodes_in_cycles()
        if nodes_in_cycles:
            response.success = False
            response.error_message = (
                f"Found cycles in tree {self.tree_structure.name} after inserting node "
                f"{request.node.name}. Nodes in cycles: {str(nodes_in_cycles)}"
            )
            # First, remove all of the node's children to avoid infinite
            # recursion in remove_node()
            for child_id in [c.node_id for c in instance.children]:
                remove_node_result = instance.remove_child(child_id)
                if remove_node_result.is_err():
                    return response

            # Then remove the node from the tree
            self.remove_node(
                request=RemoveNode.Request(
                    node_name=instance.name, remove_children=False
                ),
                response=RemoveNode.Response(),
            )
            return response
        self.publish_structure()
        return response

    @is_edit_service
    @typechecked
    def change_tree_name(
        self, request: ChangeTreeName.Request, response: ChangeTreeName.Response
    ) -> ChangeTreeName.Response:
        """Change the name of the currently loaded tree."""
        self.tree_structure.name = request.name
        self.logging_manager.set_tree_info(self.tree_id, request.name)
        self.publish_structure()

        response.success = True

        return response

    @is_edit_service
    @typechecked
    def remove_node(
        self, request: RemoveNode.Request, response: RemoveNode.Response
    ) -> RemoveNode.Response:
        """
        Remove the node identified by `request.node_name` from the tree.

        If the parent of the node removed supports enough children to
        take on all of the removed node's children, it will. Otherwise,
        children will be orphaned.
        """
        match ros_to_uuid(request.node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                node_id = n_id

        if node_id not in self.nodes:
            response.success = False
            response.error_message = (
                f"No node with id {request.node_id} in "
                f"tree {self.tree_structure.name}"
            )
            return response

        target_node = self.nodes[node_id]
        # Shutdown node - this should also shutdown all children, but you
        # never know, so check later.
        shutdown_result = target_node.shutdown()
        if shutdown_result.is_err():
            response.success = False
            response.error_message = (
                "Failed to shutdown node to remove: "
                f"{str(shutdown_result.unwrap_err())}"
            )
            return response

        node_ids_to_remove = [target_node.node_id]
        if request.remove_children:
            add_children_of = [target_node.node_id]
            while add_children_of:
                n_id = add_children_of.pop()
                if n_id not in self.nodes:
                    response.success = False
                    response.error_message = (
                        f"Error while removing children of node {target_node.name}: "
                        f"No node with id {n_id} in tree {self.tree_structure.name}"
                    )
                    return response
                node_ids_to_remove.extend(
                    [child.node_id for child in self.nodes[n_id].children]
                )
                add_children_of.extend(
                    [child.node_id for child in self.nodes[n_id].children]
                )

        # Unwire wirings that have removed nodes as source or target
        unwire_response = self.unwire_data(
            WireNodeData.Request(
                wirings=[
                    wiring
                    for wiring in self.tree_structure.data_wirings
                    # Wirings coming from the internal state will have valid node ids
                    if (
                        ros_to_uuid(wiring.source.node_id).unwrap()
                        in node_ids_to_remove
                        or ros_to_uuid(wiring.target.node_id).unwrap()
                        in node_ids_to_remove
                    )
                ]
            ),
            WireNodeData.Response(),
        )
        if not unwire_response.success:
            response.success = False
            response.error_message = (
                "Failed to unwire nodes for removal: "
                f"{unwire_response.error_message}"
            )
            return response

        # Remove nodes in the reverse order they were added to the
        # list, i.e. the "deepest" ones first. This ensures that the
        # parent we refer to in the error message still exists.

        # set to prevent us from removing a node twice - no node
        # *should* have more than one parent, but better safe than
        # sorry
        removed_node_ids = set()
        for n_id in reversed(node_ids_to_remove):
            if n_id in removed_node_ids:
                continue
            removed_node_ids.add(n_id)
            # Check if node is already in shutdown state. If not, call
            # shutdown, but warn, because the parent node should have
            # done that!
            r_node = self.nodes[n_id]
            if r_node.state != BTNodeState.SHUTDOWN:
                # It's reasonable to expect parent to not be None here, since
                # the node is one of a list of children
                if r_node.parent is None:
                    self.get_logger().error(
                        f"Node {r_node.name} appears to be a child with no parent"
                    )
                    continue
                self.get_logger().warn(
                    f"Node {r_node.name} was not shut down. "
                    "Shutdown of child nodes should be handled in the base `Node` class."
                )
                r_node.shutdown()

            # If we have a parent, remove the node from that parent
            # TODO Why this convoluted double lookup, the `parent` reference should be good?
            if (
                r_node.parent is not None
                and r_node.parent.node_id in self.nodes  # type: ignore
            ):
                self.nodes[r_node.parent.node_id].remove_child(r_node.node_id)  # type: ignore
            del self.nodes[r_node.node_id]

        # This is moved down here, because setting the parent to None breaks the unwire
        if not request.remove_children:
            # If we're not removing the children, at least set their parent to None
            for child in target_node.children:
                child.parent = None

        # Keep tree_structure up-to-date
        # TODO The unwire_data call above should already update this
        self.tree_structure.data_wirings = [
            wiring
            for wiring in self.tree_structure.data_wirings
            # Wirings coming from the internal state will have valid node ids
            if (
                ros_to_uuid(wiring.source.node_id).unwrap() not in removed_node_ids
                and ros_to_uuid(wiring.target.node_id).unwrap() not in removed_node_ids
            )
        ]

        self.tree_structure.public_node_data = [
            data
            for data in self.tree_structure.public_node_data
            # Data coming from the internal state will have valid node ids
            if ros_to_uuid(data.node_id).unwrap() not in removed_node_ids
        ]

        for n_id in removed_node_ids:
            self.subtree_manager.remove_subtree(n_id)

        response.success = True
        self.publish_structure()
        return response

    @is_edit_service
    @typechecked
    def morph_node(
        self, request: MorphNode.Request, response: MorphNode.Response
    ) -> MorphNode.Response:
        """Morphs the flow control node into the new node provided in `request.new_node`."""
        match ros_to_uuid(request.node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                node_id = n_id

        if node_id not in self.nodes:
            response.success = False
            response.error_message = (
                f"No node with id {node_id} in" f" tree {self.tree_structure.name}"
            )
            return response

        old_node = self.nodes[node_id]

        new_node_result = self.instantiate_node_from_msg(
            request.new_node, self.ros_node
        )
        if new_node_result.is_err():
            response.success = False
            response.error_message = (
                "Error instantiating node " f"{str(new_node_result.unwrap_err())}"
            )
            return response
        new_node = new_node_result.unwrap()

        # First unwire all data connection to the existing node
        wire_request = WireNodeData.Request(
            wirings=[
                wiring
                for wiring in self.tree_structure.data_wirings
                if old_node.node_id
                in [
                    ros_to_uuid(wiring.source.node_id),
                    ros_to_uuid(wiring.target.node_id),
                ]
            ]
        )

        unwire_resp = WireNodeData.Response()
        unwire_resp = self.unwire_data(request=wire_request, response=unwire_resp)
        if not get_success(unwire_resp):
            return MorphNode.Response(
                success=False,
                error_message=(
                    f"Failed to unwire data for node {old_node.name}: "
                    f"{get_error_message(unwire_resp)}"
                ),
            )

        parent = None
        if old_node.parent:
            parent = old_node.parent
            # Remember the old index so we can insert the new instance at
            # the same position
            old_child_index = parent.get_child_index(old_node.node_id)

            if old_child_index is None:
                return MorphNode.Response(
                    success=False,
                    error_message=(
                        f"Parent of node {old_node.name} claims to have no child with that name?!"
                    ),
                )
            remove_child_result = parent.remove_child(old_node.node_id)
            if remove_child_result.is_err():
                response.success = False
                response.error_message = (
                    "Could not remove child from parent: "
                    f"{str(remove_child_result.unwrap_err())}"
                )
                return response
            add_child_result = parent.add_child(new_node, at_index=old_child_index)
            if add_child_result.is_err():
                response.error_message = (
                    f"Failed to add new instance of node {old_node.name}: "
                    f"{str(add_child_result.unwrap_err())}"
                )
                add_old_node_result = parent.add_child(
                    old_node, at_index=old_child_index
                )
                if add_old_node_result.is_err():
                    response.error_message += "\n Also failed to restore old node."

                rewire_resp = WireNodeData.Response()
                rewire_resp = self.wire_data(request=wire_request, response=rewire_resp)
                if not get_success(rewire_resp):
                    response.error_message += (
                        f"\nAlso failed to restore data wirings: "
                        f"{get_error_message(rewire_resp)}"
                    )
                response.success = False
                return response

        # Move the children from old to new
        for child_id in [child.node_id for child in old_node.children]:
            remove_child_result = old_node.remove_child(child_id)
            if remove_child_result.is_err():
                response.success = False
                response.error_message = (
                    "Could not remove child from old node: "
                    f"{str(remove_child_result.unwrap_err())}"
                )
                return response
            child = remove_child_result.unwrap()
            if child_id != new_node.node_id:
                add_child_result = new_node.add_child(child)
                if add_child_result.is_err():
                    response.success = False
                    response.error_message = (
                        "Could not add child to new node: "
                        f"{str(add_child_result.unwrap_err())}"
                    )
                    return response

        # Add the new node to self.nodes
        del self.nodes[old_node.node_id]
        self.nodes[new_node.node_id] = new_node

        # Re-wire all the data, just as it was before
        # FIXME: this should be a best-effort rewiring, only re-wire identical input/outputs
        new_wire_request = deepcopy(wire_request)

        rewire_resp = WireNodeData.Response()
        rewire_resp = self.wire_data(request=new_wire_request, response=rewire_resp)
        if not get_success(rewire_resp):
            response.error_message = (
                f"Failed to re-wire data to new node {new_node.name}:"
                f" {get_error_message(rewire_resp)}"
            )
            response.success = False
            return response

        response.success = True
        self.publish_structure()
        return response

    @is_edit_service
    @typechecked
    def set_options(  # noqa: C901
        self, request: SetOptions.Request, response: SetOptions.Response
    ) -> SetOptions.Response:
        """
        Set the option values of a given node.

        This is an "edit service", i.e. it can only be used when the
        tree has not yet been initialized or has been shut down.
        """
        match ros_to_uuid(request.node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                node_id = n_id

        if node_id not in self.nodes:
            response.success = False
            response.error_message = (
                f"Unable to find node {node_id} in tree " f"{self.tree_structure.name}"
            )
            return response

        node = self.nodes[node_id]
        unknown_options = []
        preliminary_incompatible_options = []

        try:
            deserialized_options: Dict[str, Any] = {}
            for option in request.options:
                deserialized_options[option.key] = json_decode(option.serialized_value)
            # deserialized_options = {
            #    (option.key, json_decode(option.serialized_value))
            #    for option in request.options
            # }
        except ValueError as ex:
            response.success = False
            response.error_message = f"Failed to deserialize option value: {str(ex)}"
            return response

        # Find any options values that
        # a) the node does not expect
        # b) have the wrong type
        for key, value in deserialized_options.items():
            if key not in node.options:
                unknown_options.append(key)
                continue

            required_type = node.options.get_type(key)
            if not node.options.compatible(key, value):
                preliminary_incompatible_options.append((key, required_type.__name__))

        error_strings = []
        if unknown_options:
            error_strings.append(f"Unknown option keys: {str(unknown_options)}")

        incompatible_options = []
        if preliminary_incompatible_options:
            # traditionally we would fail here, but re-check if the type of an option
            # with a known option-wiring changed
            # this could mean that the previously incompatible option is actually
            # compatible with the new type!
            for key, required_type_name in preliminary_incompatible_options:
                incompatible = True
                possible_optionref = node.__class__._node_config.options[key]

                if isinstance(possible_optionref, OptionRef):
                    other_type = deserialized_options[possible_optionref.option_key]
                    our_type = type(deserialized_options[key])
                    if other_type == our_type:
                        incompatible = False
                    elif inspect.isclass(other_type):
                        deserialized_options[key] = (
                            message_converter.convert_dictionary_to_ros_message(
                                other_type, deserialized_options[key]
                            )
                        )
                        incompatible = False
                    else:
                        # check if the types are str or unicode and treat them the same
                        if (
                            isinstance(deserialized_options[key], str)
                            and other_type == str
                        ):
                            incompatible = False
                        if (
                            isinstance(deserialized_options[key], str)
                            and other_type == str
                        ):
                            incompatible = False
                if incompatible:
                    incompatible_options.append((key, required_type_name))

        if incompatible_options:
            error_strings.append(
                "Incompatible option keys:\n"
                + "\n".join(
                    [
                        f"Key {key} has type {type(deserialized_options[key]).__name__}, "
                        f"should be {required_type_name}"
                        for key, required_type_name in incompatible_options
                    ]
                )
            )
        if error_strings:
            response.success = False
            response.error_message = "\n".join(error_strings)
            return response

        # Because options are used at construction time, we need to
        # construct a new node with the new options.

        # First, we need to add the option values that didn't change
        # to our dict:
        for key in node.options:
            if key not in deserialized_options:
                deserialized_options[key] = node.options[key]

        # Now we can construct the new node - no need to call setup,
        # since we're guaranteed to be in the edit state
        # (i.e. `root.setup()` will be called before anything that
        # needs the node to be set up)
        try:
            new_node = node.__class__(
                node_id=node_id,
                options=deserialized_options,
                name=request.new_name if request.rename_node else node.name,
                debug_manager=node.debug_manager,
                subtree_manager=node.subtree_manager,
                logging_manager=self.get_logger(),
                ros_node=self.ros_node,
            )
        except BehaviorTreeException as exc:
            response.success = False
            response.error_message = str(exc)
            return response

        # Use this request to unwire any data connections the existing
        # node has - if we didn't do this, the node wouldn't ever be
        # garbage collected, among other problems.
        #
        # We'll use the same request to re-wire the connections to the
        # new node (or the old one, if anything goes wrong).
        wire_request = WireNodeData.Request(
            wirings=[
                wiring
                for wiring in self.tree_structure.data_wirings
                if node_id
                in [
                    ros_to_uuid(wiring.source.node_id).unwrap(),
                    ros_to_uuid(wiring.target.node_id).unwrap(),
                ]
            ]
        )

        unwire_resp = WireNodeData.Response()
        unwire_resp = self.unwire_data(request=wire_request, response=unwire_resp)
        if not get_success(unwire_resp):
            response.success = False
            response.error_message = (
                f"Failed to unwire data for node {node.name}: "
                f"{get_error_message(unwire_resp)}"
            )
            return response

        parent = None
        if node.parent:
            parent = node.parent
            # Remember the old index so we can insert the new instance at
            # the same position
            old_child_index = parent.get_child_index(node.node_id)

            if old_child_index is None:
                response.success = False
                response.error_message = (
                    f"Parent of node {node.name} claims to "
                    f"have no child with that name?!"
                )
                return response

            remove_child_result = parent.remove_child(node.node_id)
            if remove_child_result.is_err():
                error_message = (
                    f"Failed to remove old instance of node {node.name}: "
                    f"{str(remove_child_result.unwrap_err())}"
                )

                rewire_resp = WireNodeData.Response()
                rewire_resp = self.wire_data(request=wire_request, response=rewire_resp)

                if not get_success(rewire_resp):
                    error_message += (
                        "\nAlso failed to restore data wirings: "
                        f"{get_error_message(rewire_resp)}"
                    )

                response.success = False
                response.error_message = error_message
                return response

            add_child_result = parent.add_child(new_node, at_index=old_child_index)
            if add_child_result.is_err():
                error_message = (
                    f"Failed to add new instance of node {node.name}: "
                    f"{str(add_child_result.unwrap_err())}"
                )
                add_child_result = parent.add_child(node, at_index=old_child_index)
                if add_child_result.is_err():
                    error_message += "\n Also failed to restore old node."
                else:

                    rewire_resp = WireNodeData.Response()
                    rewire_resp = self.wire_data(
                        request=wire_request, response=rewire_resp
                    )
                    if not get_success(rewire_resp):
                        error_message += (
                            f"\nAlso failed to restore data wirings: "
                            f"{get_error_message(rewire_resp)}"
                        )
                response.success = False
                response.error_message = error_message
                return response

        # Add the new node to self.nodes
        self.nodes[node_id] = new_node

        rewire_resp = WireNodeData.Response()
        rewire_resp = self.wire_data(request=wire_request, response=rewire_resp)
        if not get_success(rewire_resp):
            error_message = (
                f"Failed to re-wire data to new node {new_node.name}: "
                f"{get_error_message(rewire_resp)}"
            )
            # Try to undo everything, starting with adding the old
            # node back to the node dict
            self.nodes[node_id] = node

            if parent is not None:
                remove_child_result = parent.remove_child(new_node.node_id)
                if remove_child_result.is_err():
                    error_message += (
                        "\nError restoring old node: "
                        f"{str(remove_child_result.unwrap_err())}"
                    )
                else:
                    add_child_result = parent.add_child(node, at_index=old_child_index)
                    if add_child_result.is_err():
                        error_message += (
                            "\nError restoring old node: "
                            f"{str(add_child_result.unwrap_err())}"
                        )

            # Now try to re-do the wirings
            recovery_wire_response = WireNodeData.Response()
            recovery_wire_response = self.wire_data(
                request=wire_request, response=recovery_wire_response
            )
            if not get_success(recovery_wire_response):
                error_message += (
                    f"\nFailed to re-wire data to restored node {node.name}: "
                    f"{get_error_message(recovery_wire_response)}"
                )
            response.success = False
            response.error_message = error_message
            return response

        # This line is important: The list comprehension creates a
        # new list that won't be affected by calling
        # remove_child()!
        for child_id in [child.node_id for child in node.children]:
            self.get_logger().info(f"Moving child {child_id}")
            remove_child_result = node.remove_child(child_id)
            if remove_child_result.is_err():
                response.success = False
                response.error_message = (
                    "Failed to transfer children to new node: "
                    f"{str(remove_child_result.unwrap_err())}"
                )
                return response
            child = remove_child_result.unwrap()
            add_child_result = new_node.add_child(child)
            if add_child_result.is_err():
                response.success = False
                response.error_message = (
                    "Failed to transfer children to new node: "
                    f"{str(remove_child_result.unwrap_err())}"
                )
                return response

        # We made it!
        self.publish_structure()
        response.success = True
        return response

    @is_edit_service
    @typechecked
    def move_node(
        self, request: MoveNode.Request, response: MoveNode.Response
    ) -> MoveNode.Response:
        """Move the named node to a different parent and insert it at the given index."""
        match ros_to_uuid(request.node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                node_id = n_id

        match ros_to_uuid(request.parent_node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(p_id):
                parent_node_id = p_id

        if node_id not in self.nodes:
            response.success = False
            response.error_message = f'Node to be moved ("{node_id}") is not in tree.'
            return response

        node = self.nodes[node_id]

        # Empty parent name -> just remove node from parent
        if parent_node_id == uuid.UUID(int=0):
            if node.parent is not None:
                remove_child_result = node.parent.remove_child(node.node_id)
                if remove_child_result.is_err():
                    response.success = False
                    response.error_message = (
                        f"Could not remove child {node.name} from {node.parent.name}: "
                        f"{str(remove_child_result.unwrap_err())}"
                    )
                    return response

            self.publish_structure()
            response.success = True
            return response

        if parent_node_id not in self.nodes:
            response.success = False
            response.error_message = f'New parent ("{parent_node_id}") is not in tree.'
            return response

        new_parent = self.nodes[parent_node_id]

        if (
            new_parent.node_config.max_children is not None
            and len(new_parent.children) == new_parent.node_config.max_children
        ):
            response.success = False
            response.error_message = (
                f"Cannot move node {node.name} to new parent node "
                f"{new_parent.name}. "
                "Parent node already has the maximum number "
                f"of children ({new_parent.node_config.max_children})."
            )
            return response

        # If the new parent is part of the moved node's subtree, we'd
        # get a cycle, so check for that and fail if true!
        get_subtree_msg_result = node.get_subtree_msg()
        if get_subtree_msg_result.is_err():
            response.success = False
            response.error_message = (
                f"Could not generate subtree msg from {node.name}: "
                f"{str(get_subtree_msg_result.unwrap_err())}"
            )
            return response
        subtree_msg = get_subtree_msg_result.unwrap()
        if new_parent.node_id in [
            subtree_node.name for subtree_node in subtree_msg[0].nodes
        ]:
            response.success = False
            response.error_message = (
                f"Cannot move node {node.name} to new parent node "
                f"{new_parent.name}. "
                f"{new_parent.name} is a child of {node.name}!"
            )
            return response

        # Remove node from old parent, if any
        old_parent = node.parent
        if old_parent is not None:
            remove_child_result = old_parent.remove_child(node.node_id)
            if remove_child_result.is_err():
                response.success = False
                response.error_message = (
                    f"Failed to remove child {node.node_id} from {old_parent.name}: "
                    f"{str(remove_child_result.unwrap_err())}"
                )
                return response

        # Add node to new parent
        add_child_result = new_parent.add_child(
            child=node, at_index=request.new_child_index
        )
        if add_child_result.is_err():
            response.success = False
            response.error_message = (
                f"Failed to add child {node.name} to {new_parent.name}: "
                f"{str(add_child_result.unwrap_err())}"
            )
            return response

        self.publish_structure()
        response.success = True
        return response

    @is_edit_service
    @typechecked
    def replace_node(
        self, request: ReplaceNode.Request, response: ReplaceNode.Response
    ) -> ReplaceNode.Response:
        """
        Replace the named node with `new_node`.

        Will also move all children of the old node to the new one, but
        only if `new_node` supports that number of children. Otherwise,
        this will return an error and leave the tree unchanged.
        """
        match ros_to_uuid(request.old_node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                old_node_id = n_id

        match ros_to_uuid(request.new_node_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                new_node_id = n_id

        if old_node_id not in self.nodes:
            response.success = False
            response.error_message = (
                f'Node to be replaced ("{old_node_id}")' "is not in tree."
            )
            return response
        if new_node_id not in self.nodes:
            response.success = False
            response.error_message = (
                f'Replacement node ("{new_node_id}") is not in tree.'
            )
            return response

        old_node = self.nodes[old_node_id]
        new_node = self.nodes[new_node_id]

        if (
            new_node.node_config.max_children is not None
            and len(old_node.children) > new_node.node_config.max_children
        ):
            response.success = False
            response.error_message = (
                f'Replacement node ("{new_node.name}") does not support the number of'
                f"children required ({old_node.name} has "
                f"{len(old_node.children)} children, "
                f"{new_node.name} supports {new_node.node_config.max_children}."
            )
            return response

        # TODO(nberg): Actually implement this

        # If the new node has inputs/outputs with the same name and
        # type as the old one,wire them the same way the old node was
        # wired

        # Note the old node's position in its parent's children array
        old_node_parent = old_node.parent
        # Initialize to 0 just to be sure. We *should* be guaranteed
        # to find the old node in its parent's children array, but
        # better safe than sorry.
        old_node_child_index = 0
        if old_node_parent is not None:
            for index, child in enumerate(old_node_parent.children):
                if child.node_id == old_node.node_id:
                    old_node_child_index = index
                    break

        # If it has the same parent as the old node, check its index, too.
        #
        # If the new node's index is smaller than the old one's, we
        # need to subtract one from old_node_child_index. Imagine we
        # want to replace B with A, and both are children of the same
        # node:
        #
        # parent.children = [A, B, C]
        #
        # Then old_node_child_index would be 1. But if we remove B
        #
        # parent.children = [A, C]
        #
        # And then move A to old_node_child_index, we end up with
        #
        # parent.children = [C, A]
        #
        # Which is wrong!
        if (
            new_node.parent is not None
            and old_node_parent is not None
            and new_node.parent.node_id == old_node_parent.node_id
            and old_node_child_index > 0
        ):
            for index, child in enumerate(new_node.parent.children):
                if child.node_id == new_node.node_id:
                    if index < old_node_child_index:
                        old_node_child_index -= 1
                    break

        # Move the children from old to new
        for child_id in [child.node_id for child in old_node.children]:
            remove_child_result = old_node.remove_child(child_id)
            if remove_child_result.is_err():
                response.success = False
                response.error_message = (
                    f"Could not remove child node: {child_id} "
                    f"{str(remove_child_result.unwrap_err())}"
                )
                return response

            child = remove_child_result.unwrap()
            if child_id != new_node.node_id:
                add_child_result = new_node.add_child(child)
                if add_child_result.is_err():
                    response.success = False
                    response.error_message = (
                        f"Failed to add child {child.name}: "
                        f"{str(add_child_result.unwrap_err())}"
                    )
                    return response

        # Remove the old node (we just moved the children, so we can
        # set remove_children to True)
        res = RemoveNode.Response()
        res = self.remove_node(
            RemoveNode.Request(
                node_id=uuid_to_ros(old_node.node_id), remove_children=True
            ),
            res,
        )

        if not get_success(res):
            # self.publish_structure()
            response.success = False
            response.error_message = (
                f'Could not remove old node: "{get_error_message(res)}"'
            )
            return response

        # Move the new node to the old node's parent (if it had one)
        if old_node_parent is not None:
            move_response = self.move_node(
                MoveNode.Request(
                    node_id=uuid_to_ros(new_node.node_id),
                    parent_node_id=uuid_to_ros(old_node_parent.node_id),
                    new_child_index=old_node_child_index,
                ),
                MoveNode.Response(),
            )
            if not move_response.success:
                response.success = move_response.success
                response.error_message = move_response.error_message
                return response

        self.publish_structure()
        response.success = True
        return response

    @is_edit_service
    @typechecked
    def wire_data(
        self, request: WireNodeData.Request, response: WireNodeData.Response
    ) -> WireNodeData.Response:
        """
        Connect the given pairs of node data to one another.

        :param ros_bt_py_msgs.srv.WireNodeDataRequest request:

        Contains a list of :class: `ros_bt_py_msgs.msg.NodeDataWiring`
        objects that model connections

        :returns: :class:`ros_bt_py_msgs.src.WireNodeDataResponse` or `None`
        """
        response.success = True
        find_root_result = self.find_root()
        if find_root_result.is_err():
            response.success = False
            response.error_message = (
                "Unable to find root node: " f"{str(find_root_result.unwrap_err())}"
            )
            return response
        root = find_root_result.unwrap()
        if not root:
            response.success = False
            response.error_message = "Tree is empty cannot wire!"
            return response

        successful_wirings = []
        for wiring in request.wirings:
            match ros_to_uuid(wiring.target.node_id):
                case Err(e):
                    response.success = False
                    response.error_message = e
                    break
                case Ok(n_id):
                    target_node_id = n_id

            target_node = root.find_node(target_node_id)
            if not target_node:
                response.success = False
                response.error_message = f"Target node {target_node_id} does not exist"
                break
            wire_data_result = target_node.wire_data(wiring)
            if wire_data_result.is_err():
                if not request.ignore_failure:
                    response.success = False
                    response.error_message = (
                        f'Failed to execute wiring "{wiring}": '
                        f"{str(wire_data_result.unwrap_err())}"
                    )
                    break
            else:
                successful_wirings.append(wiring)

        if not response.success:
            # Undo the successful wirings
            for wiring in successful_wirings:
                match ros_to_uuid(wiring.target.node_id):
                    case Err(e):
                        response.success = False
                        response.error_message = e
                        return response
                    case Ok(n_id):
                        target_node_id = n_id

                target_node = root.find_node(target_node_id)
                if not target_node:
                    response.success = False
                    response.error_message = (
                        "Failed to find node target: " f"{target_node_id}"
                    )
                    return response
                unwire_result = target_node.unwire_data(wiring)
                if unwire_result.is_err():
                    response.success = False
                    response.error_message = (
                        f'Failed to undo wiring "{wiring}": {str(unwire_result.unwrap_err())}\n'
                        f"Previous error: {response.error_message}"
                    )
                    self.get_logger().error(
                        "Failed to undo successful wiring after error. "
                        "Tree is in undefined state!"
                    )
                    return response
            return response
        else:
            # only actually wire any data if there were no errors
            # We made it here, so all the Wirings should be valid. Time to save
            # them.
            cast(list, self.tree_structure.data_wirings).extend(successful_wirings)
            self.publish_structure()
        return response

    @is_edit_service
    @typechecked
    def unwire_data(
        self, request: WireNodeData.Request, response: WireNodeData.Response
    ) -> WireNodeData.Response:
        """
        Disconnect the given pairs of node data.

        :param ros_bt_py_msgs.srv.WireNodeDataRequest request:

        Contains a list of :class:`ros_bt_py_msgs.msg.NodeDataWiring`
        objects that model connections

        :returns: :class:`ros_bt_py_msgs.src.WireNodeDataResponse` or `None`
        """
        root_result = self.find_root()
        if root_result.is_err():
            response.success = False
            response.error_message = (
                "Unable to find root node: " f"{str(root_result.unwrap_err())}"
            )
            return response

        root = root_result.unwrap()
        if not root:
            response.success = False
            response.error_message = "Tree is empty cannot unwire data!"
            return response

        response.success = True
        successful_unwirings = []
        for wiring in request.wirings:
            match ros_to_uuid(wiring.target.node_id):
                case Err(e):
                    response.success = False
                    response.error_message = e
                    break
                case Ok(n_id):
                    target_node_id = n_id

            target_node = root.find_node(target_node_id)
            if not target_node:
                response.success = False
                response.error_message = f"Target node {target_node_id} does not exist"
                break
            unwire_result = target_node.unwire_data(wiring)
            if unwire_result.is_err():
                response.success = False
                response.error_message = (
                    f'Failed to remove wiring "{wiring}": '
                    f"{str(unwire_result.unwrap_err())}"
                )
                break
            successful_unwirings.append(wiring)

        if not response.success:
            # Re-Wire the successful unwirings
            for wiring in successful_unwirings:
                match ros_to_uuid(wiring.target.node_id):
                    case Err(e):
                        response.success = False
                        response.error_message = e
                        return response
                    case Ok(n_id):
                        target_node_id = n_id

                target_node = root.find_node(target_node_id)
                if not target_node:
                    response.success = False
                    response.error_message = (
                        f"Failed to find node: {wiring.target.node_id}"
                    )
                    return response

                wire_data_result = target_node.wire_data(wiring)
                if wire_data_result.is_err():
                    response.success = False
                    response.error_message = (
                        f'Failed to redo wiring "{wiring}": {str(wire_data_result.unwrap_err())}\n'
                        f"Previous error: {response.error_message}"
                    )
                    self.get_logger().error(
                        "Failed to rewire successful unwiring after error. "
                        "Tree is in undefined state!"
                    )
                    return response
            return response
        else:
            # We've removed these NodeDataWirings, so remove them from tree_msg as
            # well.
            for wiring in request.wirings:
                if wiring in self.tree_structure.data_wirings:
                    cast(list, self.tree_structure.data_wirings).remove(wiring)
            self.publish_structure()
        return response

    @typechecked
    def get_subtree(
        self, request: GetSubtree.Request, response: GetSubtree.Response
    ) -> GetSubtree.Response:
        if request.root_id not in self.nodes:
            response.success = False
            response.error_message = f'Node "{request.root_id}" does not exist!'
            return response

        match ros_to_uuid(request.root_id):
            case Err(e):
                response.success = False
                response.error_message = e
                return response
            case Ok(n_id):
                root_id = n_id

        get_subtree_msg_result = self.nodes[root_id].get_subtree_msg()

        if get_subtree_msg_result.is_err():
            response.subtree = False
            response.error_message = (
                "Error retrieving subtree rooted at "
                f"{request.root_id}: {str(get_subtree_msg_result.unwrap_err())}"
            )
            return response

        subtree_msg = get_subtree_msg_result.unwrap()
        response.success = True
        response.subtree = subtree_msg[0]
        return response

    @typechecked
    def generate_subtree(
        self, request: GenerateSubtree.Request, response: GenerateSubtree.Response
    ) -> GenerateSubtree.Response:
        """
        Generate a subtree generated from the provided list of nodes and the loaded tree.

        This also adds all relevant parents to the tree message, resulting in a tree that is
        executable and does not contain any orpahned nodes.
        """
        whole_tree = deepcopy(self.tree_structure)

        root_result = self.find_root()
        if root_result.is_err():
            response.success = False
            response.error_message = (
                "Could not determine tree root: " f"{str(root_result.unwrap_err())}"
            )
            return response
        root = root_result.unwrap()

        if not root:
            response.success = False
            response.error_message = "No tree message available"
            return response
        nodes = set()
        for node in whole_tree.nodes:
            nodes.add(node.node_id)

        nodes_to_keep = set()
        nodes_to_remove = set()
        for node in whole_tree.nodes:
            for search_node in request.node_ids:
                if node.node_id == search_node or search_node in node.child_ids:
                    nodes_to_keep.add(node.node_id)

        for node in nodes:
            if node not in nodes_to_keep:
                nodes_to_remove.add(node)

        manager = TreeEditManager(
            ros_node=self.ros_node,
            name="temporary_tree_manager",
            debug_manager=DebugManager(ros_node=self.ros_node),
        )

        load_response = LoadTree.Response()
        load_response = manager.load_tree(
            request=LoadTree.Request(tree=whole_tree),
            response=load_response,
        )

        if load_response.success:
            for node_id in nodes_to_remove:
                manager.remove_node(
                    RemoveNode.Request(node_id=node_id, remove_children=False),
                    RemoveNode.Response(),
                )
            root_result = manager.find_root()
            if root_result.is_err():
                response.success = False
                response.error_message = (
                    "Could not determine new subtree root: "
                    f"{str(root_result.unwrap_err())}"
                )
                return response
            root = root_result.unwrap()
            if not root:
                self.get_logger().info("No nodes in tree")
            else:
                manager.tree_structure.root_id = uuid_to_ros(root.node_id)
            response.success = True
            response.tree = manager.structure_to_msg()
            return response
        else:
            response.success = False
            response.error_message = (
                "Could not load tree into the new subtree" + load_response.error_message
            )
            return response

    #########################
    # Service Handlers Done #
    #########################


@typechecked
def get_success(response: dict | Any) -> bool:
    if isinstance(response, dict):
        return response["success"]

    return response.success


@typechecked
def get_error_message(response: dict | Any) -> str:
    if isinstance(response, dict):
        return response["error_message"]

    return response.error_message
