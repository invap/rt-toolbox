# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Fundacion-Sadosky-Commercial

import argparse
import logging
import signal
import time
import pika

from rt_toolbox.config import config
from rt_toolbox.rt_events_reader import rabbitmq_server_connections
from rt_toolbox.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_rabbitmq_wrapper.rabbitmq_utility import (
    RabbitMQError
)
from rt_toolbox.utility import (
    is_valid_file_with_extension_nex,
    is_valid_file_with_extension
)


# Errors:
# -1: Output file error
# -2: RabbitMQ server setup error

def main():
    # Signal handling flags
    signal_flags = {'stop': False, 'pause': False}

    # Signal handling functions
    def sigint_handler(signum, frame):
        signal_flags['stop'] = True

    def sigtstp_handler(signum, frame):
        signal_flags['pause'] = not signal_flags['pause']  # Toggle pause state

    # Registering signal handlers
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTSTP, sigtstp_handler)

    # Argument processing
    parser = argparse.ArgumentParser(
        prog = "The Events Reader for The Runtime Monitor",
        description = "Reads events from a file and publishes them in the events exchange at a RabbitMQ server.",
        epilog = "Example: python -m rt_toolbox.rt_events_reader.rt_events_reader_sh /path/to/file --rabbitmq_config_file=./rabbitmq_config.toml --log_file=output.log --log_level=debug --timeout=120"
    )
    parser.add_argument("src_file", type=str, help="Path to the file to be read.")
    parser.add_argument("--rabbitmq_config_file", type=str, default='./rabbitmq_config.toml', help='Path to the TOML file containing the RabbitMQ server configuration.')
    parser.add_argument("--log_level", type=str, choices=["debug", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout for event acquisition from file in seconds (0 = no timeout).")
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
    logger = logging.getLogger("rt_toolbox.rt_events_reader.rt_events_reader_sh")
    logger.info(f"Log verbosity level: {logging_level}.")
    if args.log_file is None:
        logger.info("Log destination: CONSOLE.")
    else:
        if not valid_log_file:
            logger.info("Log file error. Log destination: CONSOLE.")
        else:
            logger.info(f"Log destination: FILE ({args.log_file}).")
    # Validate and normalize the input file path
    valid = is_valid_file_with_extension(args.src_file, 'any')
    if not valid:
        logger.error(f"Input file error.")
        exit(-1)
    logger.info(f"Input file: {args.src_file}")
    # Determine timeout
    config.timeout = args.timeout if args.timeout >= 0 else 0
    logger.info(f"Timeout for event acquisition from file: {config.timeout} seconds.")
    # RabbitMQ infrastructure configuration
    valid = is_valid_file_with_extension(args.rabbitmq_config_file, "toml")
    if not valid:
        logger.critical(f"RabbitMQ infrastructure configuration file error.")
        exit(-1)
    logger.info(f"RabbitMQ infrastructure configuration file: {args.rabbitmq_config_file}")
    rabbitmq_server_connections.build_rabbitmq_server_connections(args.rabbitmq_config_file)
    # Start receiving events from the RabbitMQ server
    logger.info(f"Start sending events to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
    with (open(args.src_file, "r") as input_file):
        # Start event acquisition from the file
        start_time_epoch = time.time()
        number_of_events = 0
        # Control variables
        completed = False
        stop = False
        timeout = False
        for line in input_file:
            # Handle SIGINT
            if signal_flags['stop']:
                logger.info("SIGINT received. Stopping the file reading process.")
                stop = True
            # Handle SIGTSTP
            if signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the file reading process.")
                while signal_flags['pause'] and not signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the file reading process.")
                    stop = True
                if signal_flags['pause']:
                    logger.info("SIGTSTP received. Resuming the file reading process.")
            # Timeout handling for event acquisition.
            if config.timeout != 0 and time.time() - start_time_epoch >= config.timeout:
                timeout = True
            # Finish the process if any control variable establishes it
            if stop or timeout:
                break
            cleaned_event = line.rstrip('\n\r')
            # Publish event at RabbitMQ server
            try:
                rabbitmq_server_connections.rabbitmq_event_server_connection.publish_message(
                    cleaned_event,
                    pika.BasicProperties(
                        delivery_mode=2,  # Persistent message
                    )
                )
            except RabbitMQError:
                logger.info(f"Error sending event to the exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                exit(-2)
            # Log event send
            logger.debug(f"Event sent: {cleaned_event}.")
            number_of_events += 1
        else:
            completed = True
        # Send poison pill with the events exchange at the RabbitMQ server
        try:
            rabbitmq_server_connections.rabbitmq_event_server_connection.publish_message(
                '',
                pika.BasicProperties(
                    delivery_mode=2,
                    headers={'termination': True}
                )
            )
        except RabbitMQError:
            logger.critical(f"Error sending poison pill to the exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
            exit(-2)
        else:
            logger.info(f"Poison pill sent to the exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Stop publishing events to the RabbitMQ server
        logger.info(f"Stop publishing events to the exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Close connection if it exists
        rabbitmq_server_connections.rabbitmq_event_server_connection.close()
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if completed:
            logger.info(f"Events received: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process COMPLETED, EOF reached.")
        elif timeout:
            logger.info(f"Events received: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process COMPLETED, timeout reached.")
        elif stop:
            logger.info(f"Events received: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        else:
            logger.info(f"Events received: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process STOPPED, unknown reason.")
    exit(0)


if __name__ == "__main__":
    main()
