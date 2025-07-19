# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import logging
import signal
import time
import argparse
from datetime import datetime

from rt_file_tools.logger.config import config
from rt_file_tools.logger.rabbitmq_server_configs import rabbitmq_server_config
from rt_file_tools.logger.rabbitmq_server_connections import rabbitmq_server_connection
from rt_file_tools.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_file_tools.rabbitmq_utility import (
    setup_rabbitmq,
    RabbitMQError
)
from rt_file_tools.utility import (
    is_valid_file_with_extension_nex
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
        prog = "The Logger for The Runtime Monitor.",
        description="Writes a log file with with the messages got from a RabbitMQ server.",
        epilog = "Example: python -m rt_file_tools.logger.logger_sh /path/to/file --host=https://myrabbitmq.org.ar --port=5672 --user=my_user --password=my_password --log_file=output.log --log_level=event --timeout=120"
    )
    parser.add_argument('dest_file', help='Event report file name.')
    parser.add_argument('--host', type=str, default='localhost', help='RabbitMQ logging server host.')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ logging server port.')
    parser.add_argument('--user', default='guest', help='RabbitMQ logging server user.')
    parser.add_argument('--password', default='guest', help='RabbitMQ logging server password.')
    parser.add_argument('--exchange', type=str, default='my_log_exchange', help='Name of the exchange at the RabbitMQ logging server.')
    parser.add_argument("--log_level", type=str, choices=["debug", "event", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout in seconds to wait for messages after last received message (0 = no timeout).")
    # Parse arguments
    args = parser.parse_args()
    # Set up the logging infrastructure
    # Configure logging level.
    match args.log_level:
        case "debug":
            logging_level = LoggingLevel.DEBUG
        case "event":
            logging_level = LoggingLevel.EVENT
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
        valid_log_file = is_valid_file_with_extension_nex(args.log_file, "log")
        if not valid_log_file:
            logging_destination = LoggingDestination.CONSOLE
        else:
            logging_destination = LoggingDestination.FILE
    set_up_logging()
    configure_logging_destination(logging_destination, args.log_file)
    configure_logging_level(logging_level)
    logging.info(f"Log verbosity level: {logging_level}.")
    if args.log_file is None:
        logging.info("Log destination: CONSOLE.")
    else:
        if not valid_log_file:
            logging.info("Log file error. Log destination: CONSOLE.")
        else:
            logging.info(f"Log destination: FILE ({args.log_file}).")
    # Validate and normalize the event report path
    if args.dest_file is not None:
        valid = is_valid_file_with_extension_nex(args.dest_file, "log")
        if not valid:
            logging.error(f"Output file error.")
            exit(-1)
        dest_file = args.dest_file
    else:
        dest_file = "./event_report.csv"
    logging.info(f"Output log file: {dest_file}")
    # Determine timeout
    timeout = args.timeout if args.timeout >= 0 else 0
    logging.info(f"Timeout for event reception from RabbitMQ server: {timeout} seconds.")
    # RabbitMQ server configuration
    rabbitmq_server_config.host = args.host
    rabbitmq_server_config.port = args.port
    rabbitmq_server_config.user = args.user
    rabbitmq_server_config.password = args.password
    rabbitmq_server_config.exchange = args.exchange
    # Other configuration
    config.timeout = timeout
    # Open the output file and the AMQP connection
    with open(dest_file, "w") as output_file:
        last_message_time = time.time()
        # Control variables
        poison_received = False
        # Setup RabbitMQ server
        try:
            connection, channel, queue_name = setup_rabbitmq(rabbitmq_server_config, 'log_entries')
        except RabbitMQError:
            logging.critical(f"Error setting up connection to RabbitMQ server.")
            exit(-2)
        else:
            rabbitmq_server_connection.connection = connection
            rabbitmq_server_connection.channel = channel
            rabbitmq_server_connection.exchange = rabbitmq_server_config.exchange
            rabbitmq_server_connection.queue_name = queue_name
        # Start getting events to the RabbitMQ server
        logging.info(f"Start getting log entries  from RabbitMQ server at {rabbitmq_server_config.host}:{rabbitmq_server_config.port}.")
        while not poison_received and not signal_flags['stop']:
            # Handle SIGINT
            if signal_flags['stop']:
                logging.info("SIGINT received. Stopping the event acquisition process.")
                poison_received = True
            # Handle SIGTSTP
            if signal_flags['pause']:
                logging.info("SIGTSTP received. Pausing the event acquisition process.")
                while signal_flags['pause'] and not signal_flags['stop']:
                    signal.pause()  # Efficiently wait for signals
                if signal_flags['stop']:
                    logging.info("SIGINT received. Stopping the event acquisition process.")
                    poison_received = True
                logging.info("SIGTSTP received. Resuming the event acquisition process.")
            # Timeout handling for message reception
            if 0 < config.timeout < (time.time() - last_message_time):
                logging.info(f"No event received for {config.timeout} seconds. Timeout reached.")
                poison_received = True
            # Get event from RabbitMQ
            method, properties, body = rabbitmq_server_connection.channel.basic_get(
                queue=rabbitmq_server_connection.queue_name,
                auto_ack=False
            )
            if method:  # Message exists
                # Process message
                if properties.headers and properties.headers.get('termination'):
                    # Poison pill received
                    logging.info(f"Poison pill received.")
                    poison_received = True
                else:
                    last_message_time = time.time()
                    # Event received
                    event = body.decode()
                    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    output_file.write(f"{timestamp} : {event}\n")
                    output_file.flush()
                    cleaned_event = event.rstrip('\n\r')
                    logging.log(LoggingLevel.EVENT, f"Received event: {cleaned_event}.")
                # ACK the message
                rabbitmq_server_connection.channel.basic_ack(delivery_tag=method.delivery_tag)
        # Stop getting events to the RabbitMQ server
        logging.info(f"Stop getting log entries from RabbitMQ server at {rabbitmq_server_config.host}:{rabbitmq_server_config.port}.")
        # Close connection if it exists
        if connection and connection.is_open:
            try:
                connection.close()
                logging.info(f"Connection to RabbitMQ server at {args.host}:{args.port} closed.")
            except Exception as e:
                logging.error(f"Error closing connection to RabbitMQ server at {args.host}:{args.port}: {e}.")
    exit(0)


if __name__ == "__main__":
    main()
