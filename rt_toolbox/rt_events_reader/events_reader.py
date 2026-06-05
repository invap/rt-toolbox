# Copyright (c) 2024 Fundacion Sadosky, info@fundacionsadosky.org.ar
# Copyright (c) 2024 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Fundacion-Sadosky-Commercial

import json
import threading
import time
import pika
import logging
# Create a logger for the reporter component.
logger = logging.getLogger(__name__)

from rt_toolbox.rt_events_reader.errors.events_reader_errors import EventsReaderError
from rt_toolbox.rt_events_reader import rabbitmq_server_connections
from rt_toolbox.rt_events_reader.config import config

from rt_rabbitmq_wrapper.exchange_types.event.event_dict_codec import EventDictCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_csv_codec import EventCSVCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_codec_errors import (
    EventCSVError,
    EventTypeError,
)
from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError


class EventsReader(threading.Thread):
    def __init__(self, src_file, signal_flags):
        super().__init__()
        # Open destination file and create a handler (dest_file is validated before).
        self._input_file = open(src_file, "r")
        # Signaling flags.
        self._signal_flags = signal_flags

    # Raises: EventsReaderError
    def run(self):
        # Initialize start_time_epoch for testing timeout for events acquisition from the file. Also initialize number_of_events 
        # for logging the number of events processed during the event acquisition.
        start_time_epoch = time.time()
        number_of_events = 0
        # Control variables
        control = {
            "eof_stop": False,
            "timeout_stop": False,
            "signal_stop": False,
            # The monitoring process should stop if any of the flags poison_received, signal_stop, verdict_stop or timeout_stop is set to True.
            "should_stop": lambda: control["eof_stop"] or control["signal_stop"] or control["timeout_stop"]
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

        # Log the start of the sending of events to the RabbitMQ server.
        #
        # Start sending events to the RabbitMQ server.
        logger.info(f"Start sending events to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")

        # Read events from file and send them to the RabbitMQ server until EOF is reached or a signal is received or timeout is reached.
        for line in self._input_file:
            # Update termination conditions.
            _check_signals()
            _check_timeout()
            # Finish the process if any control variable establishes it.
            if control["should_stop"]():
                break
            # Process current line from file.
            event_csv = line.rstrip("\n\r")
            # Publish event at RabbitMQ server.
            try:
                event = EventCSVCoDec.from_csv(event_csv)
            except EventCSVError:
                logger.info(f"Error parsing event csv: [ {event_csv} ].")
                raise EventsReaderError()
            try:
                event_dict = EventDictCoDec.to_dict(event)
            except EventTypeError:
                logger.info(f"Error building dictionary from event: [ {event} ].")
                raise EventsReaderError()
            try:
                rabbitmq_server_connections.rabbitmq_events_server_connection.publish_message(
                    json.dumps(event_dict, indent=4),
                    pika.BasicProperties(
                        delivery_mode=2,  # Persistent message.
                    ),
                )
            except RabbitMQError:
                logger.info(f"Error sending event to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
                raise EventsReaderError()
            # Log event send.
            logger.debug(f"Sent event: {event_dict}.")
            # Only increment number_of_events is it is a valid event.
            number_of_events += 1
        else:
            control["eof_stop"] = True
        # Send poison pill with the events exchange at the RabbitMQ server.
        try:
            rabbitmq_server_connections.rabbitmq_events_server_connection.publish_message(
                "", 
                pika.BasicProperties(
                    delivery_mode=2, 
                    headers={"termination": True}
                )
            )
        except RabbitMQError:
            logger.critical(f"Error sending poison pill to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
            raise EventsReaderError()
        else:
            logger.info(f"Poison pill sent to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
        # Stop publishing events to the RabbitMQ server.
        logger.info(f"Stop publishing events to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}.")
        # Logging the reason for stoping the verification process to the RabbitMQ server.
        if control["eof_stop"]:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process COMPLETED, EOF reached.")
        elif control["signal_stop"]:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif control["timeout_stop"]:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, timeout reached ({time.time()-start_time_epoch} secs.).")
        else:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
