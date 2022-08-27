from logging import Logger
from pathlib import Path

from networkx import DiGraph
from pandas import DataFrame

from ..datasets.networkx_graph_dataset import NetworkXGraphDataSet
from ..datasets.pandas_hdf5_dataset import PandasHDF5DataSet
from ..nodes.graph_manipulation import (
    contract_nodes_from_list_set_nid,
    identify_ntype_node_to_contract_by_text,
)
from ..nodes.nx_g_schema import (
    EdgeAttr,
    EdgeAttrKey,
    EdgeAttrs,
    EdgeTuple,
    EdgeTuples,
    EdgeType,
    NodeAttrKey,
    NodeTuple,
    NodeTuples,
    NodeType,
)


def _df_to_nx_g(  # type: ignore[no-any-unimported]
    df: DataFrame, logger: Logger
) -> DiGraph:
    # Initiate result object
    nx_g = DiGraph()

    # Pop datetime out of dataframe metadata into the result graph's metadata
    nx_g.graph.update(df.attrs)

    logger.info(f"Added the following metadata to graph:\n{nx_g.graph}")

    # Initiate a node id counter
    curr_nid: int = 0

    #
    # Collect graph elements
    #

    # Initiate containers for each node and edge type
    hashtag_tuples = NodeTuples(list_node_tuple=[])
    video_tuples = NodeTuples(list_node_tuple=[])
    author_tuples = NodeTuples(list_node_tuple=[])
    music_tuples = NodeTuples(list_node_tuple=[])
    video_to_hashtag_tuples = EdgeTuples(list_edge_tuple=[])
    author_to_video_tuples = EdgeTuples(list_edge_tuple=[])
    music_to_video_tuples = EdgeTuples(list_edge_tuple=[])

    # Parse data returned from each video query

    logger.info(f"Parsing information within a dataframe shaped {df.shape}")

    for i_video in range(len(df.index)):
        # Identify the row that contains data from one single video
        series_per_video = df.iloc[i_video, :]

        #
        # Parse nodes of different types from source data
        #

        # Collect hashtag tuple
        hashtag_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=series_per_video[NodeType.hashtag.value]
        )

        curr_nid += 1

        # Collect video tuple
        video_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=series_per_video[NodeType.video.value]
        )

        curr_nid += 1

        # Collect author tuple
        author_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=series_per_video[NodeType.author.value]
        )

        curr_nid += 1

        # Collect music tuple
        music_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=series_per_video[NodeType.music.value]
        )

        curr_nid += 1

        #
        # Collect edges of different types from source data
        #

        # Collect video-to-hashtag edge tuple
        video_to_hashtag_tuple = EdgeTuple(
            src_nid=video_tuple.nid,
            dst_nid=hashtag_tuple.nid,
            edge_attrs=EdgeAttrs(
                list_edge_attr=[
                    EdgeAttr(
                        edge_attr_key=EdgeAttrKey.etype,
                        edge_attr_val=EdgeType.video_to_hashtag.value,
                    )
                ]
            ),
        )
        author_to_video_tuple = EdgeTuple(
            src_nid=author_tuple.nid,
            dst_nid=video_tuple.nid,
            edge_attrs=EdgeAttrs(
                list_edge_attr=[
                    EdgeAttr(
                        edge_attr_key=EdgeAttrKey.etype,
                        edge_attr_val=EdgeType.author_to_video.value,
                    )
                ]
            ),
        )
        music_to_video_tuple = EdgeTuple(
            src_nid=music_tuple.nid,
            dst_nid=video_tuple.nid,
            edge_attrs=EdgeAttrs(
                list_edge_attr=[
                    EdgeAttr(
                        edge_attr_key=EdgeAttrKey.etype,
                        edge_attr_val=EdgeType.music_to_video.value,
                    )
                ]
            ),
        )

        # Add to their repsective container
        hashtag_tuples.list_node_tuple.append(hashtag_tuple)
        video_tuples.list_node_tuple.append(video_tuple)
        author_tuples.list_node_tuple.append(author_tuple)
        music_tuples.list_node_tuple.append(music_tuple)

        video_to_hashtag_tuples.list_edge_tuple.append(video_to_hashtag_tuple)
        author_to_video_tuples.list_edge_tuple.append(author_to_video_tuple)
        music_to_video_tuples.list_edge_tuple.append(music_to_video_tuple)

    # Compile all node tuples and edge tuples
    all_node_tuples = NodeTuples(
        list_node_tuple=video_tuples.list_node_tuple
        # hashtag_tuples.list_node_tuple
        + author_tuples.list_node_tuple
        + music_tuples.list_node_tuple
    )
    all_edge_tuples = EdgeTuples(
        list_edge_tuple=author_to_video_tuples.list_edge_tuple
        # video_to_hashtag_tuples.list_edge_tuple
        + music_to_video_tuples.list_edge_tuple
    )

    # Populate the result graph
    nx_g.add_nodes_from(nodes_for_adding=all_node_tuples.to_list_node_tuple_native())
    nx_g.add_edges_from(ebunch_to_add=all_edge_tuples.to_list_edge_tuple_native())

    logger.info(
        f"Pre-contraction graph has {len(nx_g.nodes())} nodes "
        f"and {len(nx_g.edges())} edges"
    )

    # Contract nodes based on common text
    for ntype in [NodeType.author, NodeType.music, NodeType.hashtag]:
        list_set_nid = identify_ntype_node_to_contract_by_text(
            nx_g=nx_g,
            ntype=ntype,
            nfeat_ntype=NodeAttrKey.ntype.value,
            nfeat_text=NodeAttrKey.text.value,
            logger=logger,
        )
        nx_g = contract_nodes_from_list_set_nid(
            nx_g=nx_g, list_set_nid=list_set_nid, logger=logger
        )

    return nx_g


def df_to_nx_g(path_df: Path, path_nx_g: Path, logger: Logger) -> None:
    """Converts API responses from DataFrame format to networkx Graph format"""
    # Data Access - Input
    df_dataset = PandasHDF5DataSet(filepath=path_df, logger=logger)
    df = df_dataset.load()

    # Task Processing
    nx_g = _df_to_nx_g(df=df, logger=logger)

    # Data Access - Output
    nx_g_dataset = NetworkXGraphDataSet(filepath=path_nx_g, logger=logger)
    nx_g_dataset.save(nx_g=nx_g)


if __name__ == "__main__":
    import argparse

    from ..nodes.base_logger import get_base_logger

    logger = get_base_logger()

    parser = argparse.ArgumentParser(
        description="Converted typed TikTok hashtag video response dataframe "
        "into a networkx graph"
    )
    parser.add_argument(
        "-pd",
        "--path_df",
        type=Path,
        required=True,
        help="Path to a dataframe with typed tiktok hashtag video response data",
    )
    parser.add_argument(
        "-png",
        "--path_nx_g",
        type=Path,
        required=True,
        help="Path to a networkx graph which contains entities and relations within "
        "TikTok hashtag video responses",
    )

    args = parser.parse_args()

    df_to_nx_g(path_df=args.path_df, path_nx_g=args.path_nx_g, logger=logger)
