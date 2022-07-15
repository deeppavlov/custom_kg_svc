import pickle
import logging
from treelib import Tree
from treelib.exceptions import NodeIDAbsentError

from kg_api.utils import loader




def create_kind(kind: str, parent: str = "Kind"):
    """Adds a given kind to the ontology_graph tree.

    Args:
      kind: kind to be added
      parent: parent of kind
    Returns:

    """
    kind = kind.capitalize()
    parent = parent.capitalize()

    branch = Tree()
    branch.create_node(kind, kind)

    tree = loader.load_ontology_graph()
    if not tree:
        tree = Tree()
        tree.create_node("Kind", "Kind")

    try:
        tree.paste(parent, branch)
        with open("kg_api/database/ontology_graph.pickle", "wb") as file:
            pickle.dump(tree, file)
    except NodeIDAbsentError:
        create_kind(kind=parent)
        create_kind(kind=kind, parent=parent)
        logging.warning("Not-in-database parent '%s'. Has been added as a child of 'Kind'", parent)
    except ValueError:
        logging.info(
            "The '%s' kind exists in database. No new kind has been created", kind
        )


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
