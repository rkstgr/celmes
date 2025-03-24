#!/usr/bin/env python3
"""
Enhanced measurement node with local buffering for offline server scenarios
"""

import time
import json
import random
import uuid
import logging
import numpy as np
import zenoh
import sqlite3
import os
import threading
from datetime import datetime
from queue import Queue, Empty
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("BufferedNode")

NODE_ID = "node-000"
BUFFER_DB_PATH = "measurement_buffer.db"
MAX_BUFFER_SIZE = 100000  # Maximum number of measurements to buffer
RETRY_INTERVAL = 10  # Seconds between retry attempts when server is offline
SERVER_TIMEOUT = 5  # Seconds to wait for server response before considering it offline
RECONNECT_RETRIES = 3  # Number of retries before declaring server offline
BUFFER_BATCH_SIZE = 50  # Number of buffered measurements to send in one batch


class BufferedNode:
    """Simulates a Raspberry Pi measurement node with offline buffering capability"""

    def __init__(self, node_id=None, num_plates=4):
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.num_plates = num_plates
        self.server_online = False
        self.publish_queue = Queue()
        self.buffer_count = 0
        self.is_sending_buffer = False
        self.stopping = False

        # Initialize environmental sensors with starting values
        self.env_data = {
            "temperature": 21.0,  # Celsius
            "humidity": 45.0,  # %
            "pressure": 1013.0,  # hPa
        }

        # Initialize simulated Pi-plates
        self.plates = []
        for plate_idx in range(num_plates):
            channels = []
            for channel_idx in range(7):
                channels.append(
                    {
                        "power": 1.05,  # mW
                        "energy": 0.0,  # Wh
                        "cell_id": None,
                    }
                )

            self.plates.append(
                {
                    "plate_id": f"plate-{plate_idx}",
                    "reference_voltage": 3.3,
                    "channels": channels,
                }
            )

        # Initialize buffer database
        self._init_buffer_db()

        # Initialize Zenoh session
        self.zenoh_session = None
        self.start_time = time.time()

        # Initialize server heartbeat tracking
        self.last_heartbeat = 0
        self.heartbeat_lock = threading.Lock()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals to ensure clean exit"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stopping = True

    def _init_buffer_db(self):
        """Initialize SQLite database for measurement buffering"""
        db_exists = os.path.exists(BUFFER_DB_PATH)
        self.conn = sqlite3.connect(BUFFER_DB_PATH, check_same_thread=False)
        self.conn.execute(
            "PRAGMA journal_mode=WAL"
        )  # Use WAL mode for better concurrency
        self.db_lock = threading.Lock()

        if not db_exists:
            with self.db_lock:
                cursor = self.conn.cursor()
                cursor.execute("""
                CREATE TABLE measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    data TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    created_at INTEGER NOT NULL
                )
                """)
                cursor.execute(
                    "CREATE INDEX idx_created_at ON measurements(created_at)"
                )
                self.conn.commit()
                logger.info(f"Created buffer database at {BUFFER_DB_PATH}")

        # Count existing buffered measurements
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM measurements")
            self.buffer_count = cursor.fetchone()[0]
            logger.info(f"Found {self.buffer_count} existing buffered measurements")

    def connect_zenoh(self, config=None):
        """Connect to Zenoh network"""
        logger.info("Connecting to Zenoh network")
        try:
            self.zenoh_session = zenoh.open(
                zenoh.Config() if config is None else config
            )

            # Initialize publisher for data
            self.data_publisher = self.zenoh_session.declare_publisher(
                f"measurement/node/{self.node_id}/data"
            )

            # Initialize subscriber for heartbeat messages
            self.heartbeat_subscriber = self.zenoh_session.declare_subscriber(
                "measurement/server/heartbeat", self._handle_heartbeat
            )

            # Subscribe to control messages
            self.control_subscriber = self.zenoh_session.declare_subscriber(
                f"measurement/node/{self.node_id}/control", self._handle_control_message
            )

            # Subscribe to acknowledgment messages (for buffer publishing)
            self.ack_subscriber = self.zenoh_session.declare_subscriber(
                f"measurement/server/ack/{self.node_id}", self._handle_server_ack
            )

            logger.info(f"Node {self.node_id} connected to Zenoh")

            # Start publisher thread
            self.publisher_thread = threading.Thread(
                target=self._publisher_worker, daemon=True
            )
            self.publisher_thread.start()

            # Start buffer processing thread
            self.buffer_thread = threading.Thread(
                target=self._process_buffer, daemon=True
            )
            self.buffer_thread.start()

            # Start server checker thread
            self.checker_thread = threading.Thread(
                target=self._check_server_status, daemon=True
            )
            self.checker_thread.start()

            # Check for initial server availability
            self._check_server_connection()
        except Exception as e:
            logger.error(f"Failed to connect to Zenoh: {str(e)}")
            raise

    def close(self):
        """Close Zenoh session and database"""
        self.stopping = True

        # Wait for threads to finish
        if hasattr(self, "publisher_thread"):
            self.publisher_thread.join(timeout=2)
        if hasattr(self, "buffer_thread"):
            self.buffer_thread.join(timeout=2)
        if hasattr(self, "checker_thread"):
            self.checker_thread.join(timeout=2)

        if self.zenoh_session:
            self.zenoh_session.close()
            logger.info("Zenoh session closed")

        if hasattr(self, "conn"):
            self.conn.close()
            logger.info("Database connection closed")

    def _update_environmental_data(self):
        """Simulate small changes in environmental data"""
        self.env_data["temperature"] += np.random.normal(0, 0.05)
        self.env_data["humidity"] += np.random.normal(0, 0.1)
        self.env_data["pressure"] += np.random.normal(0, 0.05)

        # Keep values in reasonable ranges
        self.env_data["temperature"] = max(15, min(30, self.env_data["temperature"]))
        self.env_data["humidity"] = max(30, min(70, self.env_data["humidity"]))
        self.env_data["pressure"] = max(990, min(1030, self.env_data["pressure"]))

    def _update_plate_readings(self):
        """Update power and energy readings"""
        for plate in self.plates:
            ref_voltage = plate["reference_voltage"]
            voltage_factor = ref_voltage / 3.3  # Scale with reference voltage

            for channel in plate["channels"]:
                # Generate power with Gaussian noise around 1.05mW
                base_power = 1.05 * voltage_factor
                noise = np.random.normal(0, 0.01)
                power = base_power + noise
                channel["power"] = max(0, power)

                # Update energy (Wh)
                channel["energy"] += channel["power"] / 1000 / 3600

    def _prepare_data_payload(self):
        """Prepare measurement data payload"""
        timestamp = self._format_timestamp()

        payload = {
            "node_id": self.node_id,
            "timestamp": timestamp,
            "data": {
                "environment": {
                    "temperature": round(self.env_data["temperature"], 2),
                    "humidity": round(self.env_data["humidity"], 2),
                    "pressure": round(self.env_data["pressure"], 2),
                },
                "plates": [],
            },
        }

        # Add plate data
        for plate in self.plates:
            plate_data = {
                "plate_id": plate["plate_id"],
                "reference_voltage": plate["reference_voltage"],
                "channels": [],
            }

            for idx, channel in enumerate(plate["channels"]):
                plate_data["channels"].append(
                    {
                        "channel": idx,
                        "power_mW": round(channel["power"], 3),
                        "energy_Wh": round(channel["energy"], 6),
                        "cell_id": channel["cell_id"],
                    }
                )

            payload["data"]["plates"].append(plate_data)

        return payload, timestamp

    def _publisher_worker(self):
        """Worker thread to process the publishing queue"""
        while not self.stopping:
            try:
                payload, is_buffered = self.publish_queue.get(timeout=1)

                if self.server_online:
                    try:
                        if is_buffered:
                            # Send buffered data to special topic
                            publisher = self.zenoh_session.declare_publisher(
                                f"measurement/node/{self.node_id}/buffered_data"
                            )
                            publisher.put(json.dumps(payload))
                        else:
                            # Send regular data
                            self.data_publisher.put(json.dumps(payload))

                        logger.debug(
                            f"Published data: {'buffered' if is_buffered else 'live'}"
                        )
                        self.publish_queue.task_done()
                    except Exception as e:
                        logger.error(f"Failed to publish data: {str(e)}")
                        # If not buffered data, add to buffer
                        if not is_buffered:
                            self._buffer_data(payload)
                        self.publish_queue.task_done()
                        # Connection might be down, check status
                        self._check_server_connection()
                else:
                    # If not buffered data, add to buffer
                    if not is_buffered:
                        self._buffer_data(payload)
                    self.publish_queue.task_done()
            except Empty:
                pass
            except Exception as e:
                logger.error(f"Error in publisher worker: {str(e)}")

    def _check_server_status(self):
        """Thread to periodically check server status"""
        while not self.stopping:
            try:
                current_time = time.time()
                with self.heartbeat_lock:
                    server_timeout = current_time - self.last_heartbeat > SERVER_TIMEOUT

                if self.server_online and server_timeout:
                    # Server might be down
                    logger.warning("Server heartbeat timeout, checking connection")
                    self._check_server_connection()

                # Check every second
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error checking server status: {str(e)}")

    def _check_server_connection(self):
        """Check if server is reachable by sending a ping"""
        if not self.zenoh_session:
            self.server_online = False
            return

        try:
            # Send a ping message
            ping_publisher = self.zenoh_session.declare_publisher(
                f"measurement/node/{self.node_id}/ping"
            )
            ping_publisher.put(
                json.dumps(
                    {"node_id": self.node_id, "timestamp": self._format_timestamp()}
                )
            )

            # Give server time to respond with heartbeat
            time.sleep(SERVER_TIMEOUT)

            # Check if heartbeat was received
            with self.heartbeat_lock:
                server_responsive = time.time() - self.last_heartbeat <= SERVER_TIMEOUT

            if server_responsive:
                if not self.server_online:
                    logger.info("Server is back online")
                    self.server_online = True
            else:
                if self.server_online:
                    logger.warning("Server is offline, switching to buffering mode")
                    self.server_online = False

        except Exception as e:
            logger.error(f"Error checking server connection: {str(e)}")
            self.server_online = False

    def _handle_heartbeat(self, sample):
        """Process heartbeat message from server"""
        try:
            with self.heartbeat_lock:
                self.last_heartbeat = time.time()
                if not self.server_online:
                    logger.info("Received server heartbeat, server is online")
                    self.server_online = True
        except Exception as e:
            logger.error(f"Error processing heartbeat: {str(e)}")

    def _handle_server_ack(self, sample):
        """Handle acknowledgment from server for buffered data"""
        try:
            message = json.loads(sample.payload.to_string())
            batch_id = message.get("batch_id")
            success = message.get("success", False)

            if batch_id and success:
                # Delete acknowledged data from buffer
                with self.db_lock:
                    cursor = self.conn.cursor()
                    ids = json.loads(batch_id)
                    placeholders = ",".join("?" for _ in ids)
                    cursor.execute(
                        f"DELETE FROM measurements WHERE id IN ({placeholders})", ids
                    )
                    deleted_count = cursor.rowcount
                    self.conn.commit()
                    self.buffer_count -= deleted_count
                    logger.info(
                        f"Removed {deleted_count} acknowledged measurements from buffer"
                    )
        except Exception as e:
            logger.error(f"Error processing server acknowledgment: {str(e)}")

    def _buffer_data(self, payload):
        """Store measurement data in the local buffer"""
        if self.buffer_count >= MAX_BUFFER_SIZE:
            # Buffer is full, remove oldest entries
            with self.db_lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "DELETE FROM measurements WHERE id IN (SELECT id FROM measurements ORDER BY created_at ASC LIMIT ?)",
                    (BUFFER_BATCH_SIZE,),
                )
                deleted = cursor.rowcount
                self.conn.commit()
                self.buffer_count -= deleted
                logger.warning(f"Buffer full, removed {deleted} oldest measurements")

        # Insert the new measurement
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO measurements (timestamp, data, created_at) VALUES (?, ?, ?)",
                (payload.get("timestamp"), json.dumps(payload), int(time.time())),
            )
            self.conn.commit()
            self.buffer_count += 1

        if self.buffer_count % 100 == 0:
            logger.info(f"Buffered {self.buffer_count} measurements (server offline)")

    def _process_buffer(self):
        """Process buffered measurements when server is back online"""
        while not self.stopping:
            try:
                # Check if we should attempt to send buffered data
                if (
                    self.server_online
                    and self.buffer_count > 0
                    and not self.is_sending_buffer
                ):
                    self.is_sending_buffer = True
                    logger.info(
                        f"Starting to send {self.buffer_count} buffered measurements"
                    )

                    # Process in batches to avoid overwhelming the server
                    sent_count = 0
                    while (
                        self.server_online
                        and self.buffer_count > 0
                        and not self.stopping
                    ):
                        # Get a batch of buffered measurements
                        with self.db_lock:
                            cursor = self.conn.cursor()
                            cursor.execute(
                                "SELECT id, timestamp, data FROM measurements ORDER BY created_at ASC LIMIT ?",
                                (BUFFER_BATCH_SIZE,),
                            )
                            rows = cursor.fetchall()

                        if not rows:
                            break

                        # Create batch to send
                        batch_ids = []
                        for row_id, timestamp, data_json in rows:
                            try:
                                data = json.loads(data_json)
                                batch_ids.append(row_id)
                                # Queue for sending
                                self.publish_queue.put((data, True))
                                sent_count += 1
                            except json.JSONDecodeError:
                                logger.error(
                                    f"Invalid JSON in buffered data id {row_id}"
                                )
                                # Delete invalid data
                                with self.db_lock:
                                    self.conn.execute(
                                        "DELETE FROM measurements WHERE id = ?",
                                        (row_id,),
                                    )
                                    self.conn.commit()
                                    self.buffer_count -= 1

                        # Send batch ID list for acknowledgment
                        if batch_ids:
                            batch_meta_publisher = self.zenoh_session.declare_publisher(
                                f"measurement/node/{self.node_id}/buffer_batch"
                            )
                            batch_meta_publisher.put(
                                json.dumps(
                                    {
                                        "node_id": self.node_id,
                                        "batch_id": json.dumps(batch_ids),
                                        "count": len(batch_ids),
                                        "timestamp": self._format_timestamp(),
                                    }
                                )
                            )

                            # Wait a bit before sending next batch to avoid overwhelming server
                            time.sleep(1)

                    logger.info(f"Sent {sent_count} buffered measurements")
                    self.is_sending_buffer = False
            except Exception as e:
                logger.error(f"Error processing buffer: {str(e)}")
                self.is_sending_buffer = False

            # Check buffer every few seconds
            time.sleep(RETRY_INTERVAL)

    def _handle_control_message(self, sample):
        """Handle incoming control messages"""
        try:
            message = json.loads(sample.payload.to_string())
            logger.info(f"Received control message: {message}")

            if message.get("action") == "set_reference_voltage":
                plate_id = message.get("plate_id")
                new_voltage = message.get("value")

                if plate_id and new_voltage is not None:
                    self._set_reference_voltage(plate_id, new_voltage)

                    # Send acknowledgment
                    if self.zenoh_session:
                        ack_publisher = self.zenoh_session.declare_publisher(
                            f"measurement/node/{self.node_id}/ack"
                        )
                        ack_publisher.put(
                            json.dumps(
                                {
                                    "action": "set_reference_voltage",
                                    "plate_id": plate_id,
                                    "value": new_voltage,
                                    "status": "success",
                                    "timestamp": self._format_timestamp(),
                                }
                            )
                        )

            elif message.get("action") == "assign_cell":
                plate_id = message.get("plate_id")
                channel = message.get("channel")
                cell_id = message.get("cell_id")

                if plate_id is not None and channel is not None and cell_id is not None:
                    self._assign_cell(plate_id, channel, cell_id)

                    # Send acknowledgment
                    if self.zenoh_session:
                        ack_publisher = self.zenoh_session.declare_publisher(
                            f"measurement/node/{self.node_id}/ack"
                        )
                        ack_publisher.put(
                            json.dumps(
                                {
                                    "action": "assign_cell",
                                    "plate_id": plate_id,
                                    "channel": channel,
                                    "cell_id": cell_id,
                                    "status": "success",
                                    "timestamp": self._format_timestamp(),
                                }
                            )
                        )

        except json.JSONDecodeError:
            logger.error("Failed to parse control message")
        except Exception as e:
            logger.error(f"Error handling control message: {str(e)}")

    def _set_reference_voltage(self, plate_id, voltage):
        """Set reference voltage for a specific plate"""
        if not (1.0 <= voltage <= 5.0):
            logger.warning(f"Voltage {voltage}V out of acceptable range")
            return False

        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                logger.info(f"Setting reference voltage for {plate_id} to {voltage}V")
                plate["reference_voltage"] = voltage
                return True

        logger.warning(f"Plate {plate_id} not found")
        return False

    def _assign_cell(self, plate_id, channel, cell_id):
        """Assign a cell ID to a specific channel"""
        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                if 0 <= channel < len(plate["channels"]):
                    logger.info(
                        f"Assigning cell {cell_id} to {plate_id} channel {channel}"
                    )
                    plate["channels"][channel]["cell_id"] = cell_id
                    return True
                else:
                    logger.warning(f"Invalid channel number {channel}")
                    return False

        logger.warning(f"Plate {plate_id} not found")
        return False

    def _format_timestamp(self):
        """Get current time formatted as YYYY-MM-DD HH:MM:SS +ZZZZ"""
        return time.strftime("%Y-%m-%d %H:%M:%S %z")

    def run(self):
        """Main simulation loop"""
        logger.info(
            f"Starting buffered node {self.node_id} with {self.num_plates} plates"
        )

        try:
            while not self.stopping:
                # Update data
                self._update_environmental_data()
                self._update_plate_readings()

                # Create payload and put in the publishing queue
                payload, _ = self._prepare_data_payload()
                self.publish_queue.put((payload, False))

                # Wait for next second
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        except Exception as e:
            logger.error(f"Error in simulation: {str(e)}")
        finally:
            self.close()


def main():
    """Main function"""
    # Create a buffered node
    node = BufferedNode(node_id=NODE_ID, num_plates=2)

    # Connect to Zenoh
    node.connect_zenoh()

    # Run the simulation
    node.run()


if __name__ == "__main__":
    main()
