from typing import Optional
import pickle
import logging
from treelib import Tree
from treelib.exceptions import NodeIDAbsentError, DuplicatedNodeIdError

from kg_api.utils import loader




def create_kind(kind: str, parent: str = "Kind", start_tree: Optional[Tree] = None):
    """Adds a given kind to the ontology_graph tree.

    Args:
      kind: kind to be added
      parent: parent of kind

    Returns:
      tree object representing the ontology graph after the kind creation

    """
    kind = kind.capitalize()
    parent = parent.capitalize()

    if start_tree is None:
        start_tree = loader.load_ontology_graph()
        if start_tree is None:
            start_tree = Tree()
            start_tree.create_node(tag="Kind", identifier="Kind")
    tree = start_tree

    parent_node = tree.get_node(parent)
    if parent_node is None:
        tree = create_kind(parent, "Kind", tree)
        logging.warning("Not-in-database parent '%s'. Has been added as a child of 'Kind'", parent)

    try:
        tree.create_node(kind, kind, parent=parent)
        with open("kg_api/database/ontology_graph.pickle", "wb") as file:
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
