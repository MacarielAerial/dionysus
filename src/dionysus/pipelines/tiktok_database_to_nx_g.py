from logging import Logger
from pathlib import Path
from typing import Dict

from networkx import DiGraph

from ..datasets.networkx_graph_dataset import NetworkXGraphDataSet
from ..datasets.tiktok_database_dataset import TikTokDataBase, TikTokDataBaseDataSet
from ..nodes.graph_manipulation import (
    contract_nodes_from_list_set_nid,
    identify_ntype_node_to_contract_by_nfeat,
)
from ..nodes.nx_g_schema import (
    EdgeAttrKey,
    EdgeTuple,
    EdgeTuples,
    EdgeType,
    NodeAttrKey,
    NodeTuple,
    NodeTuples,
    NodeType,
)


def _tiktok_daatabase_to_nx_g(  # type: ignore[no-any-unimported]
    tiktok_database: TikTokDataBase, logger: Logger
) -> DiGraph:
    # Initiate result object
    nx_g = DiGraph()

    # Initiate intermediate object
    mapping_original_id_to_nid: Dict[str, int] = {}

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

    logger.info(f"Parsing information from a object of type {type(tiktok_database)}")

    #
    # Parse nodes of different types from source data
    #

    for _, hashtag_series in tiktok_database.hashtag_df.iterrows():
        # Collect hashtag tuple
        single_idx_hashtag_series = hashtag_series[NodeType.hashtag.value]
        hashtag_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=single_idx_hashtag_series
        )

        curr_nid += 1

        mapping_original_id_to_nid.update(
            {single_idx_hashtag_series[NodeAttrKey.id.value]: hashtag_tuple.nid}
        )

        hashtag_tuples.list_node_tuple.append(hashtag_tuple)

    for _, video_series in tiktok_database.video_df.iterrows():
        # Collect video tuple
        single_idx_video_series = video_series[NodeType.video.value]
        video_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=single_idx_video_series
        )

        curr_nid += 1

        mapping_original_id_to_nid.update(
            {single_idx_video_series[NodeAttrKey.id.value]: video_tuple.nid}
        )

        video_tuples.list_node_tuple.append(video_tuple)

    for _, author_series in tiktok_database.author_df.iterrows():
        # Collect author tuple
        single_idx_author_series = author_series[NodeType.author.value]
        author_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=single_idx_author_series
        )

        curr_nid += 1

        mapping_original_id_to_nid.update(
            {single_idx_author_series[NodeAttrKey.id.value]: author_tuple.nid}
        )

        author_tuples.list_node_tuple.append(author_tuple)

    for _, music_series in tiktok_database.music_df.iterrows():
        # Collect music tuple
        single_idx_music_series = music_series[NodeType.music.value]
        music_tuple = NodeTuple.from_node_series(
            nid=curr_nid, node_series=single_idx_music_series
        )

        curr_nid += 1

        mapping_original_id_to_nid.update(
            {single_idx_music_series[NodeAttrKey.id.value]: music_tuple.nid}
        )

        music_tuples.list_node_tuple.append(music_tuple)

    #
    # Collect edges of different types from source data
    #

    for _, author_to_video_series in tiktok_database.author_to_video_df.iterrows():
        # Collect author-to-video edge tuple
        single_idx_author_to_video_series = author_to_video_series[
            EdgeType.author_to_video.value
        ]
        src_nid = mapping_original_id_to_nid[
            single_idx_author_to_video_series[EdgeAttrKey.src_original_id.value]
        ]
        dst_nid = mapping_original_id_to_nid[
            single_idx_author_to_video_series[EdgeAttrKey.dst_original_id.value]
        ]
        author_to_video_tuple = EdgeTuple.from_edge_series(
            src_nid=src_nid,
            dst_nid=dst_nid,
            edge_series=single_idx_author_to_video_series,
        )
        author_to_video_tuples.list_edge_tuple.append(author_to_video_tuple)

    for _, music_to_video_series in tiktok_database.music_to_video_df.iterrows():
        # Collect music-to-video edge tuple
        single_idx_music_to_video_series = music_to_video_series[
            EdgeType.music_to_video.value
        ]
        src_nid = mapping_original_id_to_nid[
            single_idx_music_to_video_series[EdgeAttrKey.src_original_id.value]
        ]
        dst_nid = mapping_original_id_to_nid[
            single_idx_music_to_video_series[EdgeAttrKey.dst_original_id.value]
        ]
        music_to_video_tuple = EdgeTuple.from_edge_series(
            src_nid=src_nid,
            dst_nid=dst_nid,
            edge_series=single_idx_music_to_video_series,
        )
        music_to_video_tuples.list_edge_tuple.append(music_to_video_tuple)

    for _, video_to_hashtag_series in tiktok_database.video_to_hashtag_df.iterrows():
        # Collect video-to-hashtag edge tuple
        single_idx_video_to_hashtag_series = video_to_hashtag_series[
            EdgeType.video_to_hashtag.value
        ]
        src_nid = mapping_original_id_to_nid[
            single_idx_video_to_hashtag_series[EdgeAttrKey.src_original_id.value]
        ]
        dst_nid = mapping_original_id_to_nid[
            single_idx_video_to_hashtag_series[EdgeAttrKey.dst_original_id.value]
        ]
        video_to_hashtag_tuple = EdgeTuple.from_edge_series(
            src_nid=src_nid,
            dst_nid=dst_nid,
            edge_series=single_idx_video_to_hashtag_series,
        )
        video_to_hashtag_tuples.list_edge_tuple.append(video_to_hashtag_tuple)

    # Compile all node tuples and edge tuples
    all_node_tuples = NodeTuples(
        list_node_tuple=video_tuples.list_node_tuple
        + hashtag_tuples.list_node_tuple
        + author_tuples.list_node_tuple
        + music_tuples.list_node_tuple
    )
    all_edge_tuples = EdgeTuples(
        list_edge_tuple=author_to_video_tuples.list_edge_tuple
        + music_to_video_tuples.list_edge_tuple
        + video_to_hashtag_tuples.list_edge_tuple
    )

    # Populate the result graph
    nx_g.add_nodes_from(nodes_for_adding=all_node_tuples.to_list_node_tuple_native())
    nx_g.add_edges_from(ebunch_to_add=all_edge_tuples.to_list_edge_tuple_native())

    logger.info(
        f"Pre-contraction graph has {len(nx_g.nodes())} nodes "
        f"and {len(nx_g.edges())} edges"
    )

    # Contract nodes based on common id
    for ntype in NodeType:
        list_set_nid = identify_ntype_node_to_contract_by_nfeat(
            nx_g=nx_g,
            ntype=ntype,
            nfeat_ntype=NodeAttrKey.ntype.value,
            nfeat_to_contract=NodeAttrKey.id.value,
            logger=logger,
        )
        nx_g = contract_nodes_from_list_set_nid(
            nx_g=nx_g, list_set_nid=list_set_nid, logger=logger
        )

    return nx_g


def tiktok_database_to_nx_g(path_df: Path, path_nx_g: Path, logger: Logger) -> None:
    """Converts API responses from DataFrame format to networkx Graph format"""
    # Data Access - Input
    tiktok_database_dataset = TikTokDataBaseDataSet(filepath=path_df, logger=logger)
    tiktok_database = tiktok_database_dataset.load()

    # Task Processing
    nx_g = _tiktok_daatabase_to_nx_g(tiktok_database=tiktok_database, logger=logger)

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

    tiktok_database_to_nx_g(
        path_df=args.path_df, path_nx_g=args.path_nx_g, logger=logger
    )
