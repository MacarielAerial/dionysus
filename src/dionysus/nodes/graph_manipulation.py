from collections import Counter
from logging import Logger
from typing import Dict, List, Set

import networkx as nx
from networkx import Graph
from networkx_query import search_nodes

from .nx_g_schema import NodeType
from .utils import group_dict_keys_by_value


def identify_ntype_node_to_contract_by_text(  # type: ignore[no-any-unimported]
    nx_g: Graph, ntype: NodeType, nfeat_ntype: str, nfeat_text: str, logger: Logger
) -> List[Set[int]]:
    # Inititate result object
    list_set_nid_to_contract: List[Set[int]] = []

    # Identify candidate nodes
    list_nid_ntype: List[int] = list(
        search_nodes(graph=nx_g, query={"==": [(nfeat_ntype,), ntype.value]})
    )

    logger.debug(f"{len(list_nid_ntype)} {ntype.value} type nodes exist in input graph")

    # Construct a mapping from node id to its respective text
    dict_nid_text: Dict[int, str] = nx.get_node_attributes(
        G=nx_g.subgraph(nodes=list_nid_ntype), name=nfeat_text
    )

    # Inverse the mapping
    dict_text_list_nid = group_dict_keys_by_value(d=dict_nid_text)

    logger.debug(
        "Frequency distribution of node contraction group sizes:\n"
        f"{Counter([len(list_nid) for list_nid in dict_text_list_nid.values()])}"
    )

    for list_nid in dict_text_list_nid.values():
        if len(list_nid) > 1:
            # Never designate a single node to contract with itself
            list_set_nid_to_contract.append(set(list_nid))

    logger.info(
        f"{len(list_set_nid_to_contract)} groups of size-two and above node groups "
        "are designated for node contraction"
    )

    return list_set_nid_to_contract


def contract_nodes_from_list_set_nid(  # type: ignore[no-any-unimported]
    nx_g: Graph, list_set_nid: List[Set[int]], logger: Logger
) -> Graph:
    logger.debug(f"Contracting {len(list_set_nid)} groups of nodes...")

    n_contraction: int = 0
    for set_nid in list_set_nid:
        sorted_list_nid = sorted(list(set_nid))
        for nid in sorted_list_nid[1:]:
            # "copy" argument has to be false otherwise
            # a new copy is returned every time
            nx_g = nx.contracted_nodes(
                nx_g, sorted_list_nid[0], nid, self_loops=True, copy=False
            )
            n_contraction += 1

    logger.debug(f"{n_contraction} contractions in total are executed")

    logger.debug(
        f"Post-contraction graph has {len(nx_g.nodes())} nodes "
        f"and {len(nx_g.edges())} edges"
    )

    return nx_g
