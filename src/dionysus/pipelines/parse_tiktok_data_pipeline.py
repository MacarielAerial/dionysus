import logging
from pathlib import Path

from dotenv import load_dotenv

from dionysus.data_interfaces.tiktok_data_interface import TikTokDataInterface
from dionysus.data_interfaces.tiktok_graph_data_interface import (
    TikTokGraphDataInterface,
)

logger = logging.getLogger(__name__)


def parse_tiktok_data_pipeline(path_tiktok_data: Path, path_tiktok_graph: Path) -> None:
    # Data Access - Input
    tiktok_data_interface = TikTokDataInterface(filepath=path_tiktok_data)
    tiktok_data = tiktok_data_interface.load()

    # Task Processing
    tiktok_graph = tiktok_data.to_tiktok_graph()

    # Data Access - Output
    tiktok_graph_data_interface = TikTokGraphDataInterface(filepath=path_tiktok_graph)
    tiktok_graph_data_interface.save(data=tiktok_graph)


if __name__ == "__main__":
    import argparse

    from dionysus.nodes.project_logging import default_logging

    default_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Parse TikTok data in JSON format into a networkx graph"
    )
    parser.add_argument(
        "-ptd",
        "--path_tiktok_data",
        type=str,
        required=True,
        help="Path to a serialised TikTokData instance",
    )
    parser.add_argument(
        "-ptg",
        "--path_tiktok_graph",
        type=Path,
        required=True,
        help="Path to a serialised networkx graph instance which contains parsed data",
    )

    args = parser.parse_args()

    parse_tiktok_data_pipeline(
        path_tiktok_data=args.path_tiktok_data, path_tiktok_graph=args.path_tiktok_graph
    )
