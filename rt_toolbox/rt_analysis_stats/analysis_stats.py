# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import json
import threading
import time
import logging

# Create a logger for the reporter component
logger = logging.getLogger(__name__)

from rt_toolbox.rt_analysis_stats.errors.analysis_stats_errors import AnalysisStatsError
from rt_toolbox.rt_analysis_stats.config import config
from rt_toolbox.rt_analysis_stats import rabbitmq_server_connections

from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_dict_codec import (
    AnalysisVerdict,
    CheckpointReachedVerdict,
    ProcessVerdict,
    PyVerdict,
    SMT2Verdict,
    SymPyVerdict,
    TaskFinishedVerdict,
    TaskStartedVerdict,
    VerdictDictCoDec,
)
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_codec_errors import (
    VerdictDictError,
    VerdictTypeError,
)


class AnalysisStats(threading.Thread):
    def __init__(self, dest_file, signal_flags):
        super().__init__()
        # Open destination file and create a handler (dest_file is validated before)
        self._output_file = open(dest_file, "w")
        # Signaling flags
        self._signal_flags = signal_flags

    # Raises: AnalysisStatsError
    def run(self):
        # Initialize last_message_time for testing timeout. This variable is updated every time a message is received from the RabbitMQ server, 
        # and it is used for determining whether the time elapsed since the reception of the last message exceeds the timeout specified in the 
        # configuration, which according to the stop policy may determine the termination of the monitoring process.
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_results = 0
        # Variables for statistics. Depending on the type of verdicts received from the RabbitMQ server, different variables are updated for 
        # computing different statistics. For example, if a verdict of type TaskStartedVerdict is received, the variable task_started is 
        # incremented by one; if a verdict of type PyVerdict with a PASS result is received, the variable passed_props is incremented by one; 
        # etc. These variables are used at the end of the monitoring process for computing and writing the analysis statistics to the output 
        # file.
        # trace = []
        task_started = 0
        task_finished = 0
        checkpoints_reached = 0
        analyzed_props = 0
        passed_props = 0
        might_fail_props = 0
        failed_props = 0
        # Initialize control flags for managing the monitoring process (i.e., stopping the monitoring process when a poison pill is received 
        # from the RabbitMQ server, when a SIGINT signal is received, when a verdict message is received from the RabbitMQ server that according 
        # to the stop policy should stop the monitoring process, or when the time elapsed since the reception of the last message exceeds the 
        # timeout specified in the configuration). The control dictionary has a method should_stop() that returns True if any of the flags 
        # poison_received, signal_stop, verdict_stop or timeout_stop is set to True, and False otherwise; this method is used for managing the 
        # execution of the monitoring process and its threads.
        control = {
            "signal_stop": False,
            "timeout_stop": False,
            "poison_received": False,
            # The monitoring process should stop if any of the flags poison_received, signal_stop, verdict_stop or timeout_stop is set to True.
            "should_stop": lambda: control["signal_stop"] or control["timeout_stop"] or control["poison_received"]
        }

        # Signal handler thread infrastructure, which updates the control dictionary with the signal_stop flag if a SIGINT is received 
        # and with the pause flag if a SIGTSTP is received. The thread runs until the monitoring process should stop according to the 
        # control dictionary.
        #
        # Funtions for determining whether the monitoring process should stop according to the reception of signals SIGINT and SIGTSTP.
        @staticmethod
        def _check_signals():
            while not control["should_stop"]():
                # Handle SIGINT.
                if self._signal_flags["stop"]:
                    logger.info("SIGINT received. Stopping the event reception process.")
                    control["signal_stop"] = True
                # Handle SIGTSTP.
                if self._signal_flags["pause"]:
                    logger.info("SIGTSTP received. Pausing the event reception process.")
                    while self._signal_flags["pause"] and not self._signal_flags["stop"]:
                        time.sleep(1)  # Efficiently wait for signals.
                    if self._signal_flags["stop"]:
                        logger.info("SIGINT received. Stopping the event reception process.")
                        control["signal_stop"] = True
                    if not self._signal_flags["pause"]:
                        logger.info("SIGTSTP received. Resuming the event reception process.")
                time.sleep(1/100000)  # Sleep to avoid busy waiting.

        # Create the signal handler thread.
        signal_thread = threading.Thread(
            target=_check_signals,
            args=(),
            daemon=True
        )
        # -- END of signal handler thread infrastructure

        # Timeout checker thread infrastructure, which updates the control dictionary with the timeout_stop flag if the time elapsed since 
        # the reception of the last message exceeds the timeout specified in the configuration. The thread runs until the monitoring process 
        # should stop according to the control dictionary.
        #
        # Function for determining whether the monitoring process should stop according to the timeout of message reception from the RabbitMQ 
        # server.
        @staticmethod
        def _check_timeout():
            while not control["should_stop"]():
                if 0 < config.timeout < (time.time() - last_message_time):
                    control["timeout_stop"] = True
                time.sleep(1/100000)  # Sleep to avoid busy waiting

        # Create the timeout checker thread.
        timeout_thread = threading.Thread(
            target=_check_timeout,
            args=(),
            daemon=True
        )
        # -- END of timeout checker thread infrastructure

        # Start the threads for checking signals, timeout and verdicts for determining termination of the monitoring process.
        #
        # Start the thread checking signals.
        signal_thread.start()
        # Start the thread checking timeout.
        timeout_thread.start()

        # Log the start of the reception of analysis results from the RabbitMQ server.
        #
        # Start receiving verdicts from the RabbitMQ server.
        logger.info(f"Start receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}.")

        # Main loop of the analysis statistics process, which receives analysis results from the RabbitMQ server and processes 
        # them until the control dictionary indicates that the monitoring process should stop.
        while not control["should_stop"]():
            # Get result from RabbitMQ
            try:
                method, properties, body = rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.get_message()
            except RabbitMQError:
                logger.error(f"Error receiving analysis result from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}.")
                raise AnalysisStatsError()
            if method:  # Message exists
                # ACK the message
                try:
                    rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.ack_message(method.delivery_tag)
                except RabbitMQError:
                    logger.error(f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ event server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}.")
                    raise AnalysisStatsError()
                # Process message
                if properties.headers and properties.headers.get("termination"):
                    # Poison pill received
                    logger.info(f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}.")
                    control["poison_received"] = True
                else:
                    if properties.headers and properties.headers.get("type"):
                        last_message_time = time.time()
                        if properties.headers.get("type") == "verdict":
                            # Verdict received
                            verdict_dict = json.loads(body.decode())
                            try:
                                verdict = VerdictDictCoDec.from_dict(verdict_dict)
                            except VerdictDictError:
                                logger.error(f"Error parsing verdict dictionary: {verdict_dict}.")
                                raise AnalysisStatsError()
                            except VerdictTypeError:
                                logger.error(f"Error building dictionary from verdict: {verdict}.")
                                raise AnalysisStatsError()
                            else:
                                if isinstance(verdict, ProcessVerdict):
                                    if isinstance(verdict, TaskStartedVerdict):
                                        task_started += 1
                                    elif isinstance(verdict, TaskFinishedVerdict):
                                        task_finished += 1
                                    elif isinstance(verdict, CheckpointReachedVerdict):
                                        checkpoints_reached += 1
                                    else:
                                        logger.error(f"Invalid process verdict subtype: {verdict}.")
                                        raise AnalysisStatsError()
                                    # trace.append(verdict)
                                elif isinstance(verdict, AnalysisVerdict):
                                    analyzed_props += 1
                                    if isinstance(verdict, PyVerdict):
                                        match verdict.verdict:
                                            case PyVerdict.VERDICT.PASS:
                                                passed_props += 1
                                            case PyVerdict.VERDICT.FAIL:
                                                failed_props += 1
                                            case _:
                                                logger.error(f"Invalid py analysis result: {verdict.verdict}.")
                                    elif isinstance(verdict, SymPyVerdict):
                                        match verdict.verdict:
                                            case SymPyVerdict.VERDICT.PASS:
                                                passed_props += 1
                                            case SymPyVerdict.VERDICT.FAIL:
                                                failed_props += 1
                                            case _:
                                                logger.error(f"Invalid sympy analysis result: {verdict.verdict}.")
                                    elif isinstance(verdict, SMT2Verdict):
                                        match verdict.verdict:
                                            case SMT2Verdict.VERDICT.PASS:
                                                passed_props += 1
                                            case SMT2Verdict.VERDICT.MIGHT_FAIL:
                                                failed_props += 1
                                            case SMT2Verdict.VERDICT.FAIL:
                                                failed_props += 1
                                            case _:
                                                logger.error(f"Invalid smt2 analysis result: {verdict.verdict}.")
                                    else:
                                        logger.error(f"Invalid analysis verdict subtype: {verdict}.")
                                        raise AnalysisStatsError()
                                else:
                                    logger.error(
                                        f"Invalid verdict type: {verdict}."
                                    )
                                    raise AnalysisStatsError()
                            # Log result reception
                            logger.debug(f"Verdict received: {verdict}.")
                            # Only increment number_of_results is it is a valid verdict (rules out poisson pill)
                            number_of_results += 1
                        else:
                            pass # Only verdict messages are expected in the analysis results queue, so if the type header is not "verdict", the message is ignored and a log entry is created for that. This allows for ignoring unexpected messages without stopping the monitoring process, while still logging their reception for debugging purposes.
                    else:
                        logger.error(f"Result type received from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port} missing.")
                        raise AnalysisStatsError()
            else:
                # No message received.
                time.sleep(1/100000)  # Sleep to avoid busy waiting.

        # Log the stop of the reception of analysis results from the RabbitMQ server.
        #
        # Stop getting analysis results from the RabbitMQ server
        logger.info(f"Stop receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_analysis_results_server_connection.server_info.port}.")

        # Write the analysis results statistics to the output file.
        self._output_file.write("--------------- Analysis Statistics ---------------\n")
        self._output_file.write(f"Processed analysis results: {number_of_results}.\n")
        self._output_file.write("---------------------------------------------------\n")
        # self._output_file.write(f"Trace run: {trace}.\n")
        # self._output_file.write(f"Trace length (events): {len(trace)}.\n")
        self._output_file.write(f"Tasks started: {task_started}.\n")
        self._output_file.write(f"Tasks finished: {task_finished}.\n")
        self._output_file.write(f"Checkpoints reached: {checkpoints_reached}.\n")
        self._output_file.write("---------------------------------------------------\n")
        self._output_file.write(f"Analyzed properties: {analyzed_props}.\n")
        if analyzed_props > 0:
            self._output_file.write(
                f"PASSED properties: {passed_props} ({passed_props * 100 / analyzed_props:.2f}%).\n"
            )
            self._output_file.write(
                f"MIGHT FAIL properties: {might_fail_props} ({might_fail_props * 100 / analyzed_props:.2f}%).\n"
            )
            self._output_file.write(
                f"FAILED properties: {failed_props} ({failed_props * 100 / analyzed_props:.2f}%).\n"
            )
        self._output_file.write("---------------------------------------------------")
        self._output_file.flush()

        # Logging the reason for stoping the results logging process.
        if control["poison_received"]:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif control["signal_stop"]:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif control["timeout_stop"]:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")

        # Wait for threads for checking signals and timeout to finish before closing the connection to the RabbitMQ server and ending 
        # the run() method, as they may be processing messages from the RabbitMQ server until the control dictionary indicates that the 
        # monitoring process should stop.
        #
        # Wait for the thread checking signals to finish.
        signal_thread.join(timeout=5)
        # Wait for the thread checking timeout to finish.
        timeout_thread.join(timeout=5)
