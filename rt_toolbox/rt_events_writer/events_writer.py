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

from rt_toolbox.rt_events_writer.config import config
from rt_toolbox.rt_events_writer import rabbitmq_server_connections
from rt_toolbox.rt_events_writer.errors.events_writer_errors import EventsWriterError

from rt_rabbitmq_wrapper.exchange_types.event.event_dict_codec import EventDictCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_csv_codec import EventCSVCoDec
from rt_rabbitmq_wrapper.exchange_types.event.event_codec_errors import (
    EventDictError,
    EventTypeError
)
from rt_rabbitmq_wrapper.rabbitmq_utility import RabbitMQError


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
        logger.info(f"Start receiving events from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # initialize last_message_time for testing timeout
        last_message_time = time.time()
        start_time_epoch = time.time()
        number_of_events = 0
        # Control variables
        poison_received = False
        stop = False
        abort = False
        while not poison_received and not stop and not abort:
            # Handle SIGINT
            if self._signal_flags['stop']:
                logger.info("SIGINT received. Stopping the event reception process.")
                stop = True
            # Handle SIGTSTP
            if self._signal_flags['pause']:
                logger.info("SIGTSTP received. Pausing the event reception process.")
                while self._signal_flags['pause'] and not self._signal_flags['stop']:
                    time.sleep(1)  # Efficiently wait for signals
                if self._signal_flags['stop']:
                    logger.info("SIGINT received. Stopping the event reception process.")
                    stop = True
                if not self._signal_flags['pause']:
                    logger.info("SIGTSTP received. Resuming the event reception process.")
            # Timeout handling for event reception
            if 0 < config.timeout < (time.time() - last_message_time):
                abort = True
            # Process event only if temination has not been decided
            if not stop and not abort:
                # Get event from RabbitMQ
                try:
                    method, properties, body = rabbitmq_server_connections.rabbitmq_event_server_connection.get_message()
                except RabbitMQError:
                    logger.error(f"Error receiving event from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                    raise EventsWriterError()
                if method:  # Message exists
                    # Process message
                    if properties.headers and properties.headers.get('termination'):
                        # Poison pill received
                        logger.info(f"Poison pill received from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                        poison_received = True
                    else:
                        last_message_time = time.time()
                        # Event received
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
                            # Log event received
                            logger.debug(f"Received event: {event_dict}.")
                            # Only increment number_of_events is it is a valid event (rules out poisson pill)
                            number_of_events += 1
                    # ACK the message
                    try:
                        rabbitmq_server_connections.rabbitmq_event_server_connection.ack_message(method.delivery_tag)
                    except RabbitMQError:
                        logger.error(f"Error sending ack to exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ event server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
                        raise EventsWriterError()
        # Stop receiving messages from the RabbitMQ server
        logger.info(f"Stop receiving events from queue {rabbitmq_server_connections.rabbitmq_event_server_connection.queue_name} - exchange {rabbitmq_server_connections.rabbitmq_event_server_connection.exchange} at the RabbitMQ server at {rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.host}:{rabbitmq_server_connections.rabbitmq_event_server_connection.server_info.port}.")
        # Logging the reason for stoping the verification process to the RabbitMQ server
        if poison_received:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process COMPLETED, poison pill received.")
        elif stop:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, SIGINT received.")
        elif abort:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, event reception timeout reached ({time.time()-last_message_time} secs.).")
        else:
            logger.info(f"Written events: {number_of_events} - Time (secs.): {time.time()-start_time_epoch:.3f} - Process STOPPED, unknown reason.")
