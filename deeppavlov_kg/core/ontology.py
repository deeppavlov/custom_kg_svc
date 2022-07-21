from typing import Optional
import pickle
import logging
from treelib import Tree
from treelib.exceptions import NodeIDAbsentError, DuplicatedNodeIdError

from deeppavlov_kg.utils import loader


class Entity(object):
    """A class to represent an entity. It's used as argument for treelib node data"""
    def __init__(self, properties: set):
        """
        Args:
          properties: the state properties an entity has or could have in future
        """
        self.properties = properties


def create_kind(
    kind: str,
    parent: str = "Kind",
    start_tree: Optional[Tree] = None,
    kind_properties: Optional[set] = None,
) -> Tree:
    """Adds a given kind to the ontology_graph tree.

    Args:
      kind: kind to be added
      parent: parent of kind
      start_tree: treelib.Tree object, to which the new kind should be added
      kind_properties: A set of properties for the created kind

    Returns:
      tree object representing the ontology graph after the kind creation

    """
    if kind_properties is None:
        kind_properties = set()
    kind = kind.capitalize()
    parent = parent.capitalize()

    if start_tree is None:
        start_tree = loader.load_ontology_graph()
        if start_tree is None:
            start_tree = Tree()
            start_tree.create_node(
                tag="Kind",
                identifier="Kind",
                data=Entity(set()),
            )
    tree = start_tree

    parent_node = tree.get_node(parent)
    if parent_node is None:
        tree = create_kind(parent, "Kind", tree)
        parent_node = tree.get_node(parent)
        logging.warning("Not-in-database parent '%s'. Has been added as a child of 'Kind'", parent)

    kind_properties.update(parent_node.data.properties) # type: ignore

    try:
        tree.create_node(
            tag=kind,
            identifier=kind,
            parent=parent,
            data=Entity(kind_properties),
        )
        with open("deeppavlov_kg/database/ontology_graph.pickle", "wb") as file:
            pickle.dump(tree, file)
    except DuplicatedNodeIdError:
        logging.info(
            "The '%s' kind exists in database. No new kind has been created", kind
        )

    return tree


def get_descendant_kinds(kind: str) -> list:
    """Returns the children kinds of a given kind."""
    tree = loader.load_ontology_graph()
    descendants = []
    if tree:
        try:
            descendants = [descendant.tag for descendant in tree.children(kind)]
        except NodeIDAbsentError:
            logging.info("Not a known kind: %s", kind)
    return descendants


def get_kind_properties(kind: str) -> Optional[set]:
    """Returns the kind properties, stored in ontology graph"""
    tree = loader.load_ontology_graph()
    if tree is not None:
        kind_node = tree.get_node(kind)
        if kind_node is not None:
            return kind_node.data.properties
    return None
