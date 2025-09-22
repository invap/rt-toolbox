# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import json
import threading
import signal
import time
# import wx
import logging
# Create a logger for the reporter component
logger = logging.getLogger(__name__)

from rt_toolbox.rt_results_logger import rabbitmq_server_connections
from rt_toolbox.rt_results_logger.config import config
from rt_toolbox.rt_results_logger.errors.results_logger_errors import ResultsLoggerError

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


def rt_results_logger_runner(dest_file):
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
    # Create analysis stats
    reporter = AnalysisStats(dest_file, signal_flags)

    def _run_results_logger():
        # Starts the monitor thread
        reporter.start()
        # Waiting for the verification process to finish, either naturally or manually.
        reporter.join()
        # Signal the wx main event loop to exit
        # wx.CallAfter(wx.GetApp().ExitMainLoop)

    # Creates the application thread for controlling the monitor
    application_thread = threading.Thread(target=_run_results_logger, daemon=True)
    # Runs the application thread
    application_thread.start()
    # Initiating the wx main event loop
    # app.MainLoop()
    # Waiting for the application thread to finish
    application_thread.join()


class AnalysisStats(threading.Thread):
    def __init__(self, dest_file, signal_flags):
        super().__init__()
        # Open destination file and create a handler (dest_file is validated before)
        self._output_file = open(dest_file, "w")
        # Signaling flags
        self._signal_flags = signal_flags

    # Raises: ResultsLoggerError
    def run(self):
        # Start receiving verdicts from the RabbitMQ server
        logger.info(f"Start receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
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
            if self._signal_flags['stop']:
                logger.info("SIGINT received. Stopping the results reception process.")
                stop = True
            # Handle SIGTSTP
            if self._signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the results reception process.")
                while self._signal_flags['pause'] and not self._signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if self._signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the results reception process.")
                    stop = True
                if not self._signal_flags['pause']:
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
                    raise ResultsLoggerError()
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
                                        raise ResultsLoggerError()
                                    except VerdictTypeError:
                                        logger.critical(f"Error building dictionary from verdict: {verdict}.")
                                        raise ResultsLoggerError()
                                    else:
                                        self._output_file.write(f"{verdict}")
                                        self._output_file.write("\n")
                                        self._output_file.flush()
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
                                        raise ResultsLoggerError()
                                    except SpecificationTypeError:
                                        logger.critical(f"Error building dictionary from specification: {specification}.")
                                        raise ResultsLoggerError()
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
                                    raise ResultsLoggerError()
                        else:
                            logger.critical(f"Result type received from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port} missing.")
                            raise ResultsLoggerError()
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_results_log_server_connection.ack_message(method.delivery_tag)
                    except RabbitMQError:
                        logger.critical(f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ results log server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                        raise ResultsLoggerError()
        # Stop getting events from the RabbitMQ server
        logger.info(f"Stop receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if poison_received:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif stop:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif timeout:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, message reception timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
