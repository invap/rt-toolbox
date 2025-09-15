# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Fundacion-Sadosky-Commercial

import argparse
import json
import logging
import tomllib
import pika

from rt_toolbox.rt_monitor_dispatcher import rabbitmq_server_connections
from rt_toolbox.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_toolbox.utility import (
    is_valid_file_with_extension_nex,
    is_valid_file_with_extension
)

from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError


# Errors:
# -1: Dispatch file error
# -2: RabbitMQ server setup error
def main():
    # Argument processing
    parser = argparse.ArgumentParser(
        prog = "The Monitor Dispatcher for The Runtime Monitor",
        description = "Reads the monitor configuration from a file and publishes them in the dispatch exchange at a RabbitMQ server.",
        epilog = "Example: python -m rt_toolbox.rt_monitor_dispatcher.rt_monitor_dispatcher_sh /path/to/file --rabbitmq_config_file=./rabbitmq_config.toml --log_file=output.log --log_level=debug"
    )
    parser.add_argument("dispatch_file", type=str, help="Path to the toml file containing the monitor configuration.")
    parser.add_argument("--rabbitmq_config_file", type=str, default='./rabbitmq_config.toml', help='Path to the TOML file containing the RabbitMQ server configuration.')
    parser.add_argument("--log_level", type=str, choices=["debug", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    # Parse arguments
    args = parser.parse_args()
    # Set up the logging infrastructure
    # Configure logging level.
    match args.log_level:
        case "debug":
            logging_level = LoggingLevel.DEBUG
        case "info":
            logging_level = LoggingLevel.INFO
        case "warnings":
            logging_level = LoggingLevel.WARNING
        case "errors":
            logging_level = LoggingLevel.ERROR
        case "critical":
            logging_level = LoggingLevel.CRITICAL
        case _:
            logging_level = LoggingLevel.INFO
    # Configure logging destination.
    if args.log_file is None:
        logging_destination = LoggingDestination.CONSOLE
    else:
        valid_log_file = is_valid_file_with_extension_nex(args.log_file, 'log')
        if not valid_log_file:
            logging_destination = LoggingDestination.CONSOLE
        else:
            logging_destination = LoggingDestination.FILE
    set_up_logging()
    configure_logging_destination(logging_destination, args.log_file)
    configure_logging_level(logging_level)
    # Create a logger for the RabbitMQ utility component
    logger = logging.getLogger("rt_toolbox.rt_monitor_dispatcher.rt_monitor_dispatcher_sh")
    logger.info(f"Log verbosity level: {logging_level}.")
    if args.log_file is None:
        logger.info("Log destination: CONSOLE.")
    else:
        if not valid_log_file:
            logger.info("Log file error. Log destination: CONSOLE.")
        else:
            logger.info(f"Log destination: FILE ({args.log_file}).")
    # Validate and normalize the input file path
    valid = is_valid_file_with_extension(args.dispatch_file, 'toml')
    if not valid:
        logger.error(f"Input file error.")
        exit(-1)
    logger.info(f"Monitor dispatch file: {args.dispatch_file}")
    # RabbitMQ infrastructure configuration
    valid = is_valid_file_with_extension(args.rabbitmq_config_file, "toml")
    if not valid:
        logger.critical(f"RabbitMQ infrastructure configuration file error.")
        exit(-1)
    logger.info(f"RabbitMQ infrastructure configuration file: {args.rabbitmq_config_file}")
    # Create RabbitMQ communication infrastructure
    rabbitmq_server_connections.build_rabbitmq_server_connections(args.rabbitmq_config_file)
    # Start sending the monitor dispatch to the RabbitMQ server
    logger.info(f"Start sending the monitor dispatch to exchange {rabbitmq_server_connections.rabbitmq_dispatch_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_dispatch_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_dispatch_server_connection.server_info.port}.")
    with (open(args.dispatch_file, "rb") as input_file):
        # ...
        try:
            dispatch_dict = tomllib.load(input_file)
        except tomllib.TOMLDecodeError:
            logger.error(f"TOML decoding of file [ {args.dispatch_file} ] failed.")
            exit(-1)
        try:
            rabbitmq_server_connections.rabbitmq_dispatch_server_connection.publish_message(
                json.dumps(dispatch_dict, indent=4),
                pika.BasicProperties(
                    delivery_mode=2,  # Persistent message
                )
            )
        except RabbitMQError:
            logger.info(
                f"Error sending monitor dispatch to exchange {rabbitmq_server_connections.rabbitmq_dispatch_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_dispatch_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_dispatch_server_connection.server_info.port}.")
            exit(-2)
        # Log monitor dispatch send
        logger.debug(f"Sent monitor dispatch: {dispatch_dict}.")
    # Stop publishing events to the RabbitMQ server
    logger.info(f"Stop sending the monitor dispatch to exchange {rabbitmq_server_connections.rabbitmq_dispatch_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_dispatch_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_dispatch_server_connection.server_info.port}.")
    # Close connection if it exists
    rabbitmq_server_connections.rabbitmq_dispatch_server_connection.close()
    exit(0)


if __name__ == "__main__":
    main()
