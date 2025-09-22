# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import argparse
import signal
import threading
# import wx
import logging
# Create a logger for the reporter component
logger = None

from rt_toolbox.rt_events_writer.errors.events_writer_errors import EventsWriterError
from rt_toolbox.rt_events_writer.events_writer import EventsWriter
from rt_toolbox.utility import (
    is_valid_file_with_extension_nex,
    is_valid_file_with_extension
)
from rt_toolbox.rt_events_writer.config import config
from rt_toolbox.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_toolbox.rt_events_writer import rabbitmq_server_connections


def rt_events_writer_runner(dest_file):
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

    # Initiating wx application
    # app = wx.App()
    # Create events writer
    reporter = EventsWriter(dest_file, signal_flags)

    def _run_events_writer():
        # Starts the monitor thread
        reporter.start()
        # Waiting for the verification process to finish, either naturally or manually.
        reporter.join()
        # Signal the wx main event loop to exit
        # wx.CallAfter(wx.GetApp().ExitMainLoop)

    # Creates the application thread for controlling the monitor
    application_thread = threading.Thread(target=_run_events_writer, daemon=True)
    # Runs the application thread
    application_thread.start()
    # Initiating the wx main event loop
    # app.MainLoop()
    # Waiting for the application thread to finish
    application_thread.join()


# Exit codes:
# -1: Input file error
# -2: RabbitMQ configuration error
# -3: Events writer error
# -4: Unexpected error
def main():
    # Argument processing
    parser = argparse.ArgumentParser(
        prog="The Events Writer for The Runtime Reporter.",
        description="Writes events received from the events exchange at a RabbitMQ server to a file.",
        epilog="Example: python -m rt_toolbox.rt_events_writer.rt_events_writer_sh /path/to/file --rabbitmq_config_file=./rabbitmq_config.toml --log_file=output.log --log_level=debug --timeout=120"
    )
    parser.add_argument('dest_file', help='Path to the file to be written.')
    parser.add_argument("--rabbitmq_config_file", type=str, default='./rabbitmq_config.toml', help='Path to the TOML file containing the RabbitMQ server configuration.')
    parser.add_argument("--log_level", type=str, choices=["debug", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout in seconds to wait for events after last received, from the RabbitMQ event server (0 = no timeout).")
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
        valid_log_file = is_valid_file_with_extension_nex(args.log_file, "log")
        if not valid_log_file:
            logging_destination = LoggingDestination.CONSOLE
        else:
            logging_destination = LoggingDestination.FILE
    set_up_logging()
    configure_logging_destination(logging_destination, args.log_file)
    configure_logging_level(logging_level)
    # Create a logger for the RabbitMQ utility component
    logger = logging.getLogger("rt_toolbox.rt_events_writer.rt_events_writer")
    logger.info(f"Log verbosity level: {logging_level}.")
    if args.log_file is None:
        logger.info("Log destination: CONSOLE.")
    else:
        if not valid_log_file:
            logger.info("Log file error. Log destination: CONSOLE.")
        else:
            logger.info(f"Log destination: FILE ({args.log_file}).")
    # Validate and normalize the event report path
    if args.dest_file is not None:
        valid = is_valid_file_with_extension_nex(args.dest_file, 'any')
        if not valid:
            logger.error(f"Output file error.")
            exit(-1)
        dest_file = args.dest_file
    else:
        dest_file = "./output_file.txt"
    logger.info(f"Output file: {dest_file}")
    # Determine timeout
    config.timeout = args.timeout if args.timeout >= 0 else 0
    logger.info(f"Timeout for event reception from RabbitMQ server: {config.timeout} seconds.")
    # RabbitMQ infrastructure configuration
    valid = is_valid_file_with_extension(args.rabbitmq_config_file, "toml")
    if not valid:
        logger.critical(f"RabbitMQ infrastructure configuration file error.")
        exit(-2)
    logger.info(f"RabbitMQ infrastructure configuration file: {args.rabbitmq_config_file}")
    # Create RabbitMQ communication infrastructure
    rabbitmq_server_connections.build_rabbitmq_server_connections(args.rabbitmq_config_file)
    # Run the rt_events_writer
    try:
        rt_events_writer_runner(dest_file)
    except EventsWriterError:
        logger.critical("Events writer error.")
        exit(-3)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}.")
        exit(-4)
    # Close connection if it exists
    rabbitmq_server_connections.rabbitmq_event_server_connection.close() 
    exit(0)


if __name__ == "__main__":
    main()
