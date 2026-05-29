# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import json
import os
import threading
import time
import logging
# Create a logger for the reporter component
logger = logging.getLogger(__name__)

from rt_toolbox.rt_results_logger import rabbitmq_server_connections
from rt_toolbox.rt_results_logger.config import config
from rt_toolbox.rt_results_logger.errors.results_logger_errors import ResultsLoggerError

from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_dict_codec import (
    VerdictDictCoDec,
)
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_csv_codec import (
    VerdictCSVCoDec,
)
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_codec_errors import (
    VerdictDictError,
    VerdictTypeError,
)
from rt_rabbitmq_wrapper.exchange_types.specification.specification_dict_codec import (
    SpecificationDictCoDec,
)
from rt_rabbitmq_wrapper.exchange_types.specification.specification_codec_errors import (
    SpecificationDictError,
    SpecificationTypeError,
)
from rt_rabbitmq_wrapper.exchange_types.specification.specification import (
    PySpecification,
    SymPySpecification,
)


class ResultsLogger(threading.Thread):
    def __init__(self, dest_file, signal_flags):
        super().__init__()
        # Open destination file and create a handler (dest_file is validated before)
        self._output_path, self._output_file = os.path.split(dest_file)
        self._output_file = open(self._output_path + "/" + self._output_file, "wb")
        # Signaling flags
        self._signal_flags = signal_flags

    # Raises: ResultsLoggerError
    def run(self):
        # Start receiving verdicts from the RabbitMQ server
        logger.info(
            f"Start receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}."
        )
        # initialize last_message_time for testing timeout
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_results = 0
        # Control variables
        control = {
            "poison_received": False,
            "signal_stop": False,
            "timeout_stop": False
        }
        while not control["poison_received"] and not control["signal_stop"] and not control["timeout_stop"]:
            # Check for signals and handle them accordingly
            ResultsLogger._handle_signals(control, self._signal_flags)
            # Check for termination due to timeout or negative verdict reception
            ResultsLogger._check_timeout(control, last_message_time)
            # Process result only if temination has not been decided
            if not control["signal_stop"] and not control["timeout_stop"]:
                # Get result from RabbitMQ
                try:
                    method, properties, body = (
                        rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.get_message()
                    )
                except RabbitMQError:
                    logger.critical(
                        f"Error receiving analysis result from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}."
                    )
                    raise ResultsLoggerError()
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get("termination"):
                        # Poison pill received
                        logger.info(
                            f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}."
                        )
                        control["poison_received"] = True
                    else:
                        if properties.headers and properties.headers.get("type"):
                            last_message_time = time.time()
                            match properties.headers.get("type"):
                                case "verdict":
                                    # Verdict received
                                    verdict_dict = json.loads(body.decode())
                                    try:
                                        verdict = VerdictDictCoDec.from_dict(verdict_dict)
                                        verdict_csv = VerdictCSVCoDec.to_csv(verdict)
                                    except VerdictDictError:
                                        logger.critical(
                                            f"Error parsing verdict dictionary: {verdict_dict}."
                                        )
                                        raise ResultsLoggerError()
                                    except VerdictTypeError:
                                        logger.critical(
                                            f"Error building dictionary from verdict: {verdict}."
                                        )
                                        raise ResultsLoggerError()
                                    else:
                                        self._output_file.write(verdict_csv.encode("unicode_escape"))
                                        self._output_file.write(b"\n")
                                        self._output_file.flush()
                                        # Log result reception
                                        logger.debug(f"Verdict received: {verdict}.")
                                        # Only increment number_of_results is it is a valid verdict (rules out poisson pill)
                                        number_of_results += 1
                                case "counterexample":
                                    # Specification received
                                    spec_dict = json.loads(body.decode())
                                    try:
                                        specification = (
                                            SpecificationDictCoDec.from_dict(spec_dict)
                                        )
                                    except SpecificationDictError:
                                        logger.critical(
                                            f"Error parsing specification dictionary: {spec_dict}."
                                        )
                                        raise ResultsLoggerError()
                                    except SpecificationTypeError:
                                        logger.critical(
                                            f"Error building dictionary from specification: {specification}."
                                        )
                                        raise ResultsLoggerError()
                                    else:
                                        if isinstance(
                                            specification, PySpecification
                                        ) or isinstance(
                                            specification, SymPySpecification
                                        ):
                                            filename = f"{specification.property_name}@{specification.timestamp}.py"
                                        else:  # isinstance(specification, SMT2Specification)
                                            filename = f"{specification.property_name}@{specification.timestamp}.smt2"
                                        with open(self._output_path + "/" + filename, "w") as spec_file:
                                            spec_file.write(specification.specification)
                                        spec_file.close()
                                case _:
                                    logger.critical(
                                        f"Result type received from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port} invalid."
                                    )
                                    raise ResultsLoggerError()
                        else:
                            logger.critical(
                                f"Result type received from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port} missing."
                            )
                            raise ResultsLoggerError()
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.ack_message(
                            method.delivery_tag
                        )
                    except RabbitMQError:
                        logger.critical(
                            f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ analysis results server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}."
                        )
                        raise ResultsLoggerError()
        # Stop getting events from the RabbitMQ server
        logger.info(
            f"Stop receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}."
        )
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if control["poison_received"]:
            logger.info(
                f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received."
            )
        elif control["signal_stop"]:
            logger.info(
                f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received."
            )
        elif control["timeout_stop"]:
            logger.info(
                f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, timeout reached ({time.time()-last_message_time} secs.)."
            )
        else:
            logger.info(
                f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason."
            )

    # Functions used to check termination of the monitoring process by signals or timeout. 
    # They update the control dictionary with the corresponding flags to indicate whether 
    # the monitoring process should be stopped or not.
    @staticmethod
    def _handle_signals(control, signal_flags):
        # Handle SIGINT
        if signal_flags["stop"]:
            logger.info("SIGINT received. Stopping the event reception process.")
            control["signal_stop"] = True
        # Handle SIGTSTP
        if signal_flags["pause"]:
            logger.info("SIGTSTP received. Pausing the event reception process.")
            while signal_flags["pause"] and not signal_flags["stop"]:
                time.sleep(1)  # Efficiently wait for signals
            if signal_flags["stop"]:
                logger.info("SIGINT received. Stopping the event reception process.")
                control["signal_stop"] = True
            if not signal_flags["pause"]:
                logger.info("SIGTSTP received. Resuming the event reception process.")
                control["signal_stop"] = False
        control["signal_stop"] = False

    @staticmethod
    def _check_timeout(control, last_message_time):
        if 0 < config.timeout < (time.time() - last_message_time):
            control["timeout_stop"] = True
