import json
from logging import Logger
from pathlib import Path

import networkx as nx
from networkx import Graph


class NetworkXGraphDataSet:
    def __init__(self, filepath: Path, logger: Logger) -> None:
        self.filepath = filepath
        self.logger = logger

    def save(self, nx_g: Graph) -> None:  # type: ignore[no-any-unimported]
        self._save(filepath=self.filepath, nx_g=nx_g, logger=self.logger)

    @staticmethod
    def _save(  # type: ignore[no-any-unimported]
        filepath: Path, nx_g: Graph, logger: Logger
    ) -> None:
        with open(filepath, "w") as f:
            data = nx.node_link_data(G=nx_g)
            json.dump(data, f)

            logger.info(f"Saved a {type(nx_g)} type object to {filepath}")

    def load(self) -> Graph:  # type: ignore[no-any-unimported]
        return self._load(filepath=self.filepath, logger=self.logger)

    @staticmethod
    def _load(  # type: ignore[no-any-unimported]
        filepath: Path, logger: Logger
    ) -> Graph:
        with open(filepath, "r") as f:
            data = json.load(f)
            nx_g = nx.node_link_graph(data)

            logger.info(f"Loaded a {type(nx_g)} type object from {filepath}")

            return nx_g
