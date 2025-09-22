# Copyright (c) 2024 Fundacion Sadosky, info@fundacionsadosky.org.ar
# Copyright (c) 2024 INVAP, open@invap.com.ar
# SPDX-License-Identifier: AGPL-3.0-or-later OR Fundacion-Sadosky-Commercial

import json
import threading
import time
import pika
import logging
# Create a logger for the reporter component
logger = logging.getLogger(__name__)

from rt_toolbox.rt_events_reader.errors.events_reader_errors import EventsReaderError
from rt_toolbox.rt_events_reader import rabbitmq_server_connections
from rt_toolbox.rt_events_reader.config import config

from rt_rabbitmq_wrapper.exchange_types.event.event_dict_codec import EventDictCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_csv_codec import EventCSVCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_codec_errors import (
    EventCSVError,
    EventTypeError
)
from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError


class EventsReader(threading.Thread):
    def __init__(self, src_file, signal_flags):
        super().__init__()
        # Open destination file and create a handler (dest_file is validated before)
        self._input_file = open(src_file, "r")
        # Signaling flags
        self._signal_flags = signal_flags

    # Raises: EventsReaderError
    def run(self):
        # Start sending events to the RabbitMQ server
        logger.info(f"Start sending events to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Start event acquisition from the file
        start_time_epoch = time.time()
        number_of_events = 0
        # Control variables
        completed = False
        stop = False
        timeout = False
        for line in self._input_file:
            # Handle SIGINT
            if self._signal_flags['stop']:
                logger.info("SIGINT received. Stopping the file reading process.")
                stop = True
            # Handle SIGTSTP
            if self._signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the file reading process.")
                while self._signal_flags['pause'] and not self._signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if self._signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the file reading process.")
                    stop = True
                if self._signal_flags['pause']:
                    logger.info("SIGTSTP received. Resuming the file reading process.")
            # Timeout handling for event acquisition.
            if config.timeout != 0 and time.time() - start_time_epoch >= config.timeout:
                timeout = True
            # Finish the process if any control variable establishes it
            if stop or timeout:
                break
            event_csv = line.rstrip('\n\r')
            # Publish event at RabbitMQ server
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
                rabbitmq_server_connections.rabbitmq_event_server_connection.publish_message(
                    json.dumps(event_dict, indent=4),
                    pika.BasicProperties(
                        delivery_mode=2,  # Persistent message
                    )
                )
            except RabbitMQError:
                logger.info(
                    f"Error sending event to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                raise EventsReaderError()
            # Log event send
            logger.debug(f"Sent event: {event_dict}.")
            # Only increment number_of_events is it is a valid event
            number_of_events += 1
        else:
            completed = True
        # Send poison pill with the events exchange at the RabbitMQ server
        try:
            rabbitmq_server_connections.rabbitmq_event_server_connection.publish_message(
                '',
                pika.BasicProperties(
                    delivery_mode=2,
                    headers={'termination': True}
                )
            )
        except RabbitMQError:
            logger.critical(f"Error sending poison pill to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
            raise EventsReaderError()
        else:
            logger.info(f"Poison pill sent to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Stop publishing events to the RabbitMQ server
        logger.info(f"Stop publishing events to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if completed:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process COMPLETED, EOF reached.")
        elif timeout:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process COMPLETED, timeout reached.")
        elif stop:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        else:
            logger.info(f"Events read: {number_of_events} - Time (secs.): {time.time() - start_time_epoch:.3f} - Process STOPPED, unknown reason.")
