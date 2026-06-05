# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import json
import threading
import time
import logging
# Create a logger for the reporter component.
logger = logging.getLogger(__name__)

from rt_toolbox.rt_events_writer.config import config
from rt_toolbox.rt_events_writer import rabbitmq_server_connections
from rt_toolbox.rt_events_writer.errors.events_writer_errors import EventsWriterError

from rt_rabbitmq_wrapper.exchange_types.event.event_dict_codec import EventDictCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_csv_codec import EventCSVCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_codec_errors import (
    EventDictError,
    EventTypeError,
)
from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError


class EventsWriter(threading.Thread):
    def __init__(self, dest_file, signal_flags):
        super().__init__()
        # Open destination file and create a handler (dest_file is validated before).
        self._output_file = open(dest_file, "wb")
        # Signaling flags.
        self._signal_flags = signal_flags

    # Raises: ReporterError
    def run(self):
        # Initialize last_message_time for testing timeout. This variable is updated every time a message is received from the RabbitMQ server, 
        # and it is used for determining whether the time elapsed since the reception of the last message exceeds the timeout specified in the 
        # configuration, which according to the stop policy may determine the termination of the monitoring process.
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_events = 0
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
        # Function for determining whether the monitoring process should stop according to the reception of signals SIGINT and SIGTSTP.
        def _check_signals():
            # Handle SIGINT.
            if self._signal_flags["stop"].is_set():
                logger.info("SIGINT received. Stopping the event reception process.")
                control["signal_stop"] = True
            # Handle SIGTSTP.
            if self._signal_flags["pause"].is_set():
                logger.info("SIGTSTP received. Pausing the event reception process.")
                while self._signal_flags["pause"].is_set() and not self._signal_flags["stop"].is_set():
                    time.sleep(1/1000)  # Efficiently wait for signals.
                if self._signal_flags["stop"].is_set():
                    logger.info("SIGINT received. Stopping the event reception process.")
                    control["signal_stop"] = True
                if not self._signal_flags["pause"].is_set():
                    logger.info("SIGTSTP received. Resuming the event reception process.")
        #
        # def _check_signals():
        #     while not control["should_stop"]():
        #         # Handle SIGINT.
        #         if self._signal_flags["stop"].is_set():
        #             logger.info("SIGINT received. Stopping the event reception process.")
        #             control["signal_stop"] = True
        #         # Handle SIGTSTP.
        #         if self._signal_flags["pause"].is_set():
        #             logger.info("SIGTSTP received. Pausing the event reception process.")
        #             while self._signal_flags["pause"].is_set() and not self._signal_flags["stop"].is_set():
        #                 time.sleep(1/1000)  # Efficiently wait for signals.
        #             if self._signal_flags["stop"].is_set():
        #                 logger.info("SIGINT received. Stopping the event reception process.")
        #                 control["signal_stop"] = True
        #             if not self._signal_flags["pause"].is_set():
        #                 logger.info("SIGTSTP received. Resuming the event reception process.")
        #         time.sleep(1/1000)  # Sleep to avoid busy waiting.
        # 
        # Create the signal handler thread.
        # signal_thread = threading.Thread(
        #     target=_check_signals,
        #     args=(),
        #     daemon=True
        # )
        # -- END of signal handler thread infrastructure.

        # Timeout checker thread infrastructure, which updates the control dictionary with the timeout_stop flag if the time elapsed since 
        # the reception of the last message exceeds the timeout specified in the configuration. The thread runs until the monitoring process 
        # should stop according to the control dictionary.
        #
        # Function for determining whether the monitoring process should stop according to the timeout of message reception from the RabbitMQ
        # server.
        def _check_timeout():
            if 0 < config.timeout < (time.time() - start_time_epoch):
                control["timeout_stop"] = True
                logger.info("Timeout reached. Stopping the event reception process.")
        # 
        # def _check_timeout():
        #     while not control["should_stop"]():
        #         if 0 < config.timeout < (time.time() - start_time_epoch):
        #             control["timeout_stop"] = True
        #             logger.info("Timeout reached. Stopping the event reception process.")
        #         time.sleep(1/1000)  # Sleep to avoid busy waiting.
        # 
        # Create the timeout checker thread.
        # timeout_thread = threading.Thread(
        #     target=_check_timeout,
        #     args=(),
        #     daemon=True
        # )
        # -- END of timeout checker thread infrastructure

        # Start the threads for checking signals, timeout and verdicts for determining termination of the monitoring process.
        #
        # Start the thread checking signals.
        # signal_thread.start()
        # Start the thread checking timeout.
        # timeout_thread.start()

        # Log the start of receiving events from the RabbitMQ server.
        #
        # Start receiving events from the RabbitMQ server.
        logger.info(f"Start receiving events from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")

        # Main loop of the event writing process, which receives events from the RabbitMQ server and processes 
        # them until the control dictionary indicates that the monitoring process should stop.
        while not control["should_stop"]():
            # Update termination conditions.
            _check_signals()
            _check_timeout()
            # Get event from RabbitMQ.
            try:
                method, properties, body = rabbitmq_server_connections.rabbitmq_events_server_connection.get_message()
            except RabbitMQError:
                logger.error(f"Error receiving event from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
                raise EventsWriterError()
            if method:  # Message exists.
                # Process message.
                if properties.headers and properties.headers.get("termination"):
                    # Poison pill received.
                    logger.info(f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
                    control["poison_received"] = True
                else:
                    last_message_time = time.time()
                    # Event received.
                    event_dict = json.loads(body.decode())
                    try:
                        event = EventDictCoDec.from_dict(event_dict)
                        event_csv = EventCSVCoDec.to_csv(event)
                    except EventDictError:
                        logger.error(f"Error parsing event dictionary: {event_dict}.")
                        raise EventsWriterError()
                    except EventTypeError:
                        logger.error(f"Error building dictionary from event: {event}.")
                        raise EventsWriterError()
                    else:
                        self._output_file.write(event_csv.encode("unicode_escape"))
                        self._output_file.write(b"\n")
                        self._output_file.flush()
                        # Log event received.
                        logger.debug(f"Received event: {event}.")
                        # Only increment number_of_events is it is a valid event (rules out poisson pill).
                        number_of_events += 1
                # ACK the message.
                try:
                    rabbitmq_server_connections.rabbitmq_events_server_connection.ack_message(method.delivery_tag)
                except RabbitMQError:
                    logger.error(f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ events server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
                    raise EventsWriterError()
        # Stop receiving messages from the RabbitMQ server.
        logger.info(f"Stop receiving events from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
        # Logging the reason for stoping the verification process to the RabbitMQ server.
        if control["poison_received"]:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif control["signal_stop"]:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif control["timeout_stop"]:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
