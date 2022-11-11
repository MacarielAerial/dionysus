import logging
from collections import Counter
from typing import Any, Dict, List

import networkx as nx
from networkx import Graph

from dionysus.data_interfaces.tiktok_graph_data_interface import NodeAttrKey

logger = logging.getLogger(__name__)


def contract_graph_by_node_attr(  # type: ignore[no-any-unimported]
    nx_g: Graph, node_attr_key: NodeAttrKey
) -> Graph:
    # Initiate the intermediate variable
    attr_val_list_nid: Dict[Any, List[int]] = {}

    # Group node ids with identical attribute values together
    for nid, attr_val in nx.get_node_attributes(
        G=nx_g, name=node_attr_key.value
    ).items():
        if attr_val not in attr_val_list_nid.keys():
            attr_val_list_nid.update({attr_val: [nid]})
        else:
            attr_val_list_nid[attr_val].append(nid)

    # Report frequency distribution of numbers of group members
    attr_val_len: Dict[Any, int] = {
        attr_val: len(list_nid) for attr_val, list_nid in attr_val_list_nid.items()
    }
    counter = Counter(list(attr_val_len.values()))

    logger.debug(
        "The frequency distribution of numbers of "
        f"group members to be contracted is:\n{counter}"
    )

    # Remove single member group
    for attr_val in list(attr_val_list_nid.keys()):
        list_nid = attr_val_list_nid[attr_val]
        if len(list_nid) == 1:
            del attr_val_list_nid[attr_val]

    # Execute node contraction
    for list_nid in attr_val_list_nid.values():
        for nid in list_nid[1:]:
            nx.contracted_nodes(G=nx_g, u=list_nid[0], v=nid, copy=False)

    logger.info(
        f"Post contraction graph has {nx_g.number_of_nodes} nodes "
        f"and {nx_g.number_of_edges()} edges"
    )

    return nx_g


def contract_graphs_by_node_attr(  # type: ignore[no-any-unimported]
    *nx_g: Graph, node_attr_key: NodeAttrKey
) -> Graph:
    # Collect input graphlets
    list_nx_g: List[Graph] = [*nx_g]  # type: ignore[no-any-unimported]

    logger.info(f"{len(list_nx_g)} graphlets are set to be concatenated")

    # Concatenate all graphs into one
    concat_nx_g = nx.disjoint_union_all(graphs=[*nx_g])

    logger.info(
        f"Concatenated graphlets have {concat_nx_g.number_of_nodes()} nodes "
        f"and {concat_nx_g.number_of_edges()} edges"
    )

    contracted_nx_g = contract_graph_by_node_attr(
        nx_g=concat_nx_g, node_attr_key=node_attr_key
    )

    return contracted_nx_g
