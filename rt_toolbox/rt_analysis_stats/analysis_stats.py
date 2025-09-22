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

from rt_toolbox.rt_analysis_stats.errors.analysis_stats_errors import AnalysisStatsError
from rt_toolbox.rt_analysis_stats.config import config
from rt_toolbox.rt_analysis_stats import rabbitmq_server_connections

from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError

from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_dict_codec import VerdictDictCoDec
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict_codec_errors import (
    VerdictDictError,
    VerdictTypeError
)
from rt_rabbitmq_wrapper.exchange_types.verdict.verdict import (
    ProcessVerdict,
    TaskStartedVerdict,
    TaskFinishedVerdict,
    CheckpointReachedVerdict,
    AnalysisVerdict,
    PyVerdict,
    SymPyVerdict,
    SMT2Verdict
)


def rt_analysis_stats_runner(dest_file):
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

    def _run_analysis_stats():
        # Starts the monitor thread
        reporter.start()
        # Waiting for the verification process to finish, either naturally or manually.
        reporter.join()
        # Signal the wx main event loop to exit
        # wx.CallAfter(wx.GetApp().ExitMainLoop)

    # Creates the application thread for controlling the monitor
    application_thread = threading.Thread(target=_run_analysis_stats, daemon=True)
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

    # Raises: AnalysisStatsError
    def run(self):
        # Start receiving events from the RabbitMQ server
        logger.info(f"Start receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
        # Variables in the log analysis
        trace = []
        task_started = 0
        task_finished = 0
        checkpoints_reached = 0
        analyzed_props = 0
        passed_props = 0
        might_fail_props = 0
        failed_props = 0
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
                    logger.error(f"Error receiving analysis result from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                    raise AnalysisStatsError()
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get('termination'):
                        # Poison pill received
                        logger.info(f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                        poison_received = True
                    else:
                        if properties.headers and properties.headers.get('type'):
                            last_message_time = time.time()
                            if properties.headers.get('type') == 'verdict':
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
                                        trace.append(verdict)
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
                                        logger.error(f"Invalid verdict type: {verdict}.")
                                        raise AnalysisStatsError()
                                # Log result reception
                                logger.debug(f"Verdict received: {verdict}.")
                                # Only increment number_of_results is it is a valid verdict (rules out poisson pill)
                                number_of_results += 1
                            else:
                                pass
                        else:
                            logger.error(f"Result type received from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port} missing.")
                            raise AnalysisStatsError()
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_results_log_server_connection.ack_message(method.delivery_tag)
                    except RabbitMQError:
                        logger.error(f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ event server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
                        raise AnalysisStatsError()
        # Stop getting events from the RabbitMQ server
        logger.info(f"Stop receiving analysis results from queue {rabbitmq_server_connections.rabbitmq_results_log_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_results_log_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_results_log_server_connection.server_info.port}.")
        # Write the analysis results
        self._output_file.write("--------------- Analysis Statistics ---------------\n")
        self._output_file.write(f"Processed analysis results: {number_of_results}.\n")
        self._output_file.write("---------------------------------------------------\n")
        self._output_file.write(f"Trace run: {trace}.\n")
        self._output_file.write(f"Trace length (events): {len(trace)}.\n")
        self._output_file.write(f"Tasks started: {task_started}.\n")
        self._output_file.write(f"Tasks finished: {task_finished}.\n")
        self._output_file.write(f"Checkpoints reached: {checkpoints_reached}.\n")
        self._output_file.write("---------------------------------------------------\n")
        self._output_file.write(f"Analyzed properties: {analyzed_props}.\n")
        if analyzed_props > 0:
            self._output_file.write(f"PASSED properties: {passed_props} ({passed_props * 100 / analyzed_props:.2f}%).\n")
            self._output_file.write(f"MIGHT FAIL properties: {might_fail_props} ({might_fail_props * 100 / analyzed_props:.2f}%).\n")
            self._output_file.write(f"FAILED properties: {failed_props} ({failed_props * 100 / analyzed_props:.2f}%).\n")
        self._output_file.write("---------------------------------------------------")
        self._output_file.flush()
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if poison_received:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif stop:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif timeout:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, message reception timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Processed analysis results: {number_of_results} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
