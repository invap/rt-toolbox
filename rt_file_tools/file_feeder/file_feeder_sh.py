import argparse
import logging
import time
import pika
import signal

from rt_file_tools.config import config
from rt_file_tools.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_file_tools.rabbitmq_server_config import rabbitmq_server_config
from rt_file_tools.rabbitmq_utility import (
    rabbitmq_connect_to_server, RabbitMQError
)
from rt_file_tools.utility import is_valid_file_with_extension_nex, is_valid_file_with_extension


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
        prog = "The File Feeder for The Runtime Monitor",
        description = "Reports events from an event report file in cvs format by publishing the to a RabbitMQ server.",
        epilog = "Example: python -m rt_file_tools.file_feeder.file_feeder_sh /path/to/file --host=https://myrabbitmq.org.ar --port=5672 --user=my_user --password=my_password --log_file=output_log.txt --log_level=event --timeout=120"
    )
    parser.add_argument("src_file", type=str, help="Path to the file containing the events in cvs format.")
    parser.add_argument('--host', type=str, default='localhost', help='RabbitMQ server host.')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ server port.')
    parser.add_argument('--user', default='guest', help='RabbitMQ server user.')
    parser.add_argument('--password', default='guest', help='RabbitMQ server password.')
    parser.add_argument('--exchange', type=str, default='my_exchange', help='Name of the exchange at the RabbitMQ server.')
    parser.add_argument("--log_level", type=str, choices=["debug", "event", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout for the event acquisition process in seconds (0 = no timeout).")
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
    # Validate and normalize the input file path
    valid = is_valid_file_with_extension(args.src_file, "csv")
    if not valid:
        logging.error(f"Input file error.")
        exit(-1)
    logging.info(f"Event report file: {args.src_file}")
    # Determine timeout
    timeout = args.timeout if args.timeout >= 0 else 0
    logging.info(f"Timeout for event acquisition from the file: {timeout} seconds.")
    # RabbitMQ server configuration
    rabbitmq_server_config.host = args.host
    rabbitmq_server_config.port = args.port
    rabbitmq_server_config.user = args.user
    rabbitmq_server_config.password = args.password
    rabbitmq_server_config.exchange = args.exchange
    # Other configuration
    config.timeout = timeout
    #Start event acquisition from the file
    start_time_epoch = time.time()
    with (open(args.src_file, "r") as input_file):
        try:
            connection, channel = rabbitmq_connect_to_server()
        except RabbitMQError:
            logging.critical(f"Error setting up connection to RabbitMQ server.")
            exit(-2)
        # Start publishing events to the RabbitMQ server
        logging.info(f"Start publishing events to RabbitMQ server at {args.host}:{args.port}.")
        for line in input_file:
            # Handle SIGINT
            if signal_flags['stop']:
                logging.info("SIGINT received. Stopping the event acquisition process.")
                break
            # Handle SIGTSTP
            if signal_flags['pause']:
                logging.info("SIGTSTP received. Pausing the event acquisition process.")
                while signal_flags['pause'] and not signal_flags['stop']:
                    signal.pause()  # Efficiently wait for signals
                if signal_flags['stop']:
                    logging.info("SIGINT received. Stopping the event acquisition process.")
                    break
                logging.info("SIGTSTP received. Resuming the event acquisition process.")
            # Timeout handling for event acquisition.
            if config.timeout != 0 and time.time() - start_time_epoch >= config.timeout:
                logging.info(f"Acquired events for {config.timeout} seconds. Timeout reached.")
                break
            # Publish event at RabbitMQ server
            channel.basic_publish(
                exchange=rabbitmq_server_config.exchange,
                routing_key='events',
                body=line,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Persistent message
                )
            )
            cleaned_event = line.rstrip('\n\r')
            logging.log(LoggingLevel.EVENT, f"Sent event: {cleaned_event}.")
        # Always attempt to send poison pill if the channel is available
        channel.basic_publish(
            exchange=rabbitmq_server_config.exchange,
            routing_key='events',
            body='',
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent message
                headers={'termination': True}
            )
        )
        logging.info("Poison pill sent.")
        # Stop publishing events to the RabbitMQ server
        logging.info(f"Stop publishing events to RabbitMQ server at {args.host}:{args.port}.")
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
