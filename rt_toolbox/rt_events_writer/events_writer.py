# Copyright (c) 2025 Carlos Gustavo Lopez Pombo, clpombo@gmail.com
# Copyright (c) 2025 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Lopez-Pombo-Commercial

import json
import threading
import time
import logging
# Create a logger for the reporter component
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
        # Open destination file and create a handler (dest_file is validated before)
        self._output_file = open(dest_file, "wb")
        # Signaling flags
        self._signal_flags = signal_flags

    # Raises: ReporterError
    def run(self):
        # Start receiving events from the RabbitMQ server
        logger.info(
            f"Start receiving events from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}."
        )
        # initialize last_message_time for testing timeout
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_events = 0
        # Control variables
        control = {
            "poison_received": False,
            "timeout_stop": False,
            "signal_stop": False
        }
        while not control["poison_received"] and not control["signal_stop"] and not control["timeout_stop"]:
            # Check for signals and handle them accordingly
            EventsWriter._handle_signals(control, self._signal_flags)
            # Check for termination due to timeout or negative verdict reception
            EventsWriter._check_timeout(control, last_message_time)
            # Process event only if temination has not been decided
            if not control["signal_stop"] and not control["timeout_stop"]:
                # Get event from RabbitMQ
                try:
                    method, properties, body = (
                        rabbitmq_server_connections.rabbitmq_events_server_connection.get_message()
                    )
                except RabbitMQError:
                    logger.error(
                        f"Error receiving event from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}."
                    )
                    raise EventsWriterError()
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get("termination"):
                        # Poison pill received
                        logger.info(
                            f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}."
                        )
                        control["poison_received"] = True
                    else:
                        last_message_time = time.time()
                        # Event received
                        event_dict = json.loads(body.decode())
                        try:
                            event = EventDictCoDec.from_dict(event_dict)
                            event_csv = EventCSVCoDec.to_csv(event)
                        except EventDictError:
                            logger.error(
                                f"Error parsing event dictionary: {event_dict}."
                            )
                            raise EventsWriterError()
                        except EventTypeError:
                            logger.error(
                                f"Error building dictionary from event: {event}."
                            )
                            raise EventsWriterError()
                        else:
                            self._output_file.write(event_csv.encode("unicode_escape"))
                            self._output_file.write(b"\n")
                            self._output_file.flush()
                            # Log event received
                            logger.debug(f"Received event: {event}.")
                            # Only increment number_of_events is it is a valid event (rules out poisson pill)
                            number_of_events += 1
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_events_server_connection.ack_message(
                            method.delivery_tag
                        )
                    except RabbitMQError:
                        logger.error(
                            f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ events server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}."
                        )
                        raise EventsWriterError()
        # Stop receiving messages from the RabbitMQ server
        logger.info(
            f"Stop receiving events from queue {rabbitmq_server_connections.rabbitmq_events_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_events_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_events_server_connection.server_info.port}."
        )
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if control["poison_received"]:
            logger.info(
                f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received."
            )
        elif control["signal_stop"]:
            logger.info(
                f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received."
            )
        elif control["timeout_stop"]:
            logger.info(
                f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, timeout reached ({time.time()-last_message_time} secs.)."
            )
        else:
            logger.info(
                f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason."
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

