# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import json
import logging
import signal
import time
import argparse

from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_dict_codec import VerdictDictCoDec
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_codec_errors import (
    VerdictDictError,
    VerdictTypeError
)
from rt_rabbitmq_wrapper.exchange_types.specification.specification_dict_codec import SpecificationDictCoDec
from rt_rabbitmq_wrapper.exchange_types.specification.specification_codec_errors import (
    SpecificationDictError,
    SpecificationTypeError
)
from rt_rabbitmq_wrapper.exchange_types.specification.specification import (
    PySpecification,
    SymPySpecification
)

from rt_toolbox.utility import (
    is_valid_file_with_extension_nex,
    is_valid_file_with_extension
)
from rt_toolbox.rt_results_logger.config import config
from rt_toolbox.logging_configuration import (
    LoggingLevel,
    LoggingDestination,
    set_up_logging,
    configure_logging_destination,
    configure_logging_level
)
from rt_toolbox.rt_results_logger import rabbitmq_server_connections


# Errors:
# -1: Output file error
# -2: RabbitMQ server setup error
# -3: Analysis result error
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
        prog = "The Analysis Results logger for The Runtime Monitor.",
        description="Logs the analysis results received from a RabbitMQ server to files.",
        epilog = "Example: python -m rt_toolbox.rt_results_logger.rt_results_logger_sh /path/to/file --rabbitmq_config_file=./rabbitmq_config.toml --log_level=debug --timeout=120"
    )
    parser.add_argument('dest_file', help='Log analysis file name.')
    parser.add_argument("--rabbitmq_config_file", type=str, default='./rabbitmq_config.toml', help='Path to the TOML file containing the RabbitMQ server configuration.')
    parser.add_argument("--log_level", type=str, choices=["debug", "info", "warnings", "errors", "critical"], default="info", help="Log verbosity level.")
    parser.add_argument('--log_file', help='Path to log file.')
    parser.add_argument("--timeout", type=int, default=0, help="Timeout in seconds to wait for results after last received, from the RabbitMQ results log server (0 = no timeout).")
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
    logger = logging.getLogger("rt_toolbox.rt_results_logger.rt_results_logger_sh")
    logger.info(f"Log verbosity level: {logging_level}.")
    if args.log_file is None:
        logger.info("Log destination: CONSOLE.")
    else:
        if not valid_log_file:
            logger.info("Log file error. Log destination: CONSOLE.")
        else:
            logger.info(f"Log destination: FILE ({args.log_file}).")
    # Validate and normalize the log file path
    if args.dest_file is not None:
        valid = is_valid_file_with_extension_nex(args.dest_file, 'any')
        if not valid:
            logger.critical(f"Output log file error.")
            exit(-1)
        dest_file = args.dest_file
    else:
        dest_file = "./results_log.txt"
    logger.info(f"Output analysis log file: {dest_file}")
    # Determine timeout
    config.timeout = args.timeout if args.timeout >= 0 else 0
    logger.info(f"Timeout for results reception from RabbitMQ logging server: {config.timeout} seconds.")
    # RabbitMQ infrastructure configuration
    valid = is_valid_file_with_extension(args.rabbitmq_config_file, "toml")
    if not valid:
        logger.critical(f"RabbitMQ infrastructure configuration file error.")
        exit(-1)
    logger.info(f"RabbitMQ infrastructure configuration file: {args.rabbitmq_config_file}")
    rabbitmq_server_connections.build_rabbitmq_server_connections(args.rabbitmq_config_file)
    # Start receiving verdicts from the RabbitMQ server
    logger.info(f"Start receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
    # Open the output file and the AMQP connection
    with open(dest_file, "w") as output_file:
        # initialize last_message_time for testing timeout
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_results = 0
        # Control variables
        poison_received = False
        stop = False
        timeout = False
        while not poison_received and not stop and not timeout:
            # Handle SIGINT
            if signal_flags['stop']:
                logger.info("SIGINT received. Stopping the results reception process.")
                stop = True
            # Handle SIGTSTP
            if signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the results reception process.")
                while signal_flags['pause'] and not signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the results reception process.")
                    stop = True
                if not signal_flags['pause']:
                    logger.info("SIGTSTP received. Resuming the results reception process.")
            # Timeout handling for result reception
            if 0 < config.timeout < (time.time() - last_message_time):
                timeout = True
            # Process result only if temination has not been decided
            if not stop and not timeout:
                # Get result from RabbitMQ
                try:
                    method, properties, body = rabbitmq_server_connections.rabbitmq_results_log_server_connection.get_message()
                except RabbitMQError:
                    logger.critical(f"Error receiving analysis result from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                    exit(-2)
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get('termination'):
                        # Poison pill received
                        logger.info(f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                        poison_received = True
                    else:
                        if properties.headers and properties.headers.get('type'):
                            last_message_time = time.time()
                            match properties.headers.get('type'):
                                case 'verdict':
                                    # Verdict received
                                    verdict_dict = json.loads(body.decode())
                                    try:
                                        verdict = VerdictDictCoDec.from_dict(verdict_dict)
                                    except VerdictDictError:
                                        logger.critical(f"Error parsing verdict dictionary: {verdict_dict}.")
                                        exit(-3)
                                    except VerdictTypeError:
                                        logger.critical(f"Error building dictionary from verdict: {verdict}.")
                                        exit(-3)
                                    else:
                                        output_file.write(f"{verdict}")
                                        output_file.write("\n")
                                        output_file.flush()
                                        # Log result reception
                                        logger.debug(f"Verdict received: {verdict}.")
                                        # Only increment number_of_results is it is a valid verdict (rules out poisson pill)
                                        number_of_results += 1
                                case 'counterexample':
                                    # Specification received
                                    spec_dict = json.loads(body.decode())
                                    try:
                                        specification = SpecificationDictCoDec.from_dict(spec_dict)
                                    except SpecificationDictError:
                                        logger.critical(f"Error parsing specification dictionary: {spec_dict}.")
                                        exit(-3)
                                    except SpecificationTypeError:
                                        logger.critical(f"Error building dictionary from specification: {specification}.")
                                        exit(-3)
                                    else:
                                        if isinstance(specification, PySpecification) or isinstance(specification, SymPySpecification):
                                            filename = f"{specification.property_name}@{specification.timestamp}.py"
                                        else: # isinstance(specification, SMT2Specification)
                                            filename = f"{specification.property_name}@{specification.timestamp}.smt2"
                                        with open(filename, "w") as spec_file:
                                            spec_file.write(specification.specification)
                                        spec_file.close()
                                case _:
                                    logger.critical(f"Result type received from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port} invalid.")
                                    exit(-2)
                        else:
                            logger.critical(f"Result type received from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port} missing.")
                            exit(-2)
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_results_log_server_connection.ack_message(method.delivery_tag)
                    except RabbitMQError:
                        logger.critical(f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ results log server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                        exit(-2)
        # Stop getting events from the RabbitMQ server
        logger.info(f"Stop receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
        # Close connection if it exists
        rabbitmq_server_connections.rabbitmq_results_log_server_connection.close()
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if poison_received:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif stop:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif timeout:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, message reception timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
    exit(0)


if __name__ == "__main__":
    main()
