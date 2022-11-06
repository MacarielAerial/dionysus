from logging import Logger


def test_default_logging(test_logger: Logger) -> None:
    test_logger.info("Base logger obtained successfully")

    assert isinstance(test_logger, Logger)
