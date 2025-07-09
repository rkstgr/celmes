#!/usr/bin/env python3
"""
generic_node.py
Generic measurement node implementation with local buffering for offline server scenarios
"""

import time
import json
import uuid
import logging
import sqlite3
import os
import threading
from datetime import datetime
from queue import Queue, Empty
import signal
import abc
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("GenericNode")

# Constants
MAX_BUFFER_SIZE = 100000  # Maximum number of measurements to buffer
RETRY_INTERVAL = 10  # Seconds between retry attempts when server is offline
SERVER_TIMEOUT = 5  # Seconds to wait for server response before considering it offline
RECONNECT_RETRIES = 3  # Number of retries before declaring server offline
BUFFER_BATCH_SIZE = 50  # Number of buffered measurements to send in one batch


class GenericNode(abc.ABC):
    """Generic measurement node with offline buffering capability"""

    def __init__(
        self, node_id=None, num_plates=4, buffer_db_path="measurement_buffer.db"
    ):
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.num_plates = num_plates
        self.buffer_db_path = buffer_db_path
        self.server_online = False
        self.publish_queue = Queue()
        self.buffer_count = 0
        self.is_sending_buffer = False
        self.stopping = False
        self.array_size = 10
        self.i = 0
        self.calibration_running = False

        # Initialize environmental sensors with starting values
        self.env_data = {
            "temperature": np.zeros(self.array_size),  # Celsius
            "humidity": np.zeros(self.array_size),  # %
            "pressure": np.zeros(self.array_size),  # hPa
        }

        # Initialize plates - to be populated by child classes
        self.plates = []

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
        db_exists = os.path.exists(self.buffer_db_path)
        self.conn = sqlite3.connect(self.buffer_db_path, check_same_thread=False)
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
                logger.info(f"Created buffer database at {self.buffer_db_path}")

        # Count existing buffered measurements
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM measurements")
            self.buffer_count = cursor.fetchone()[0]
            logger.info(f"Found {self.buffer_count} existing buffered measurements")

    def connect_zenoh(self, config=None):
        """Connect to Zenoh network by scanning the subnet for the server"""
        import socket
        import zenoh

        def is_zenoh_server(ip, port=7447, timeout=0.2):
            try:
                with socket.create_connection((ip, port), timeout=timeout):
                    return True
            except Exception:
                return False

        def find_zenoh_server(subnet="192.168.42.", port=7447):
            logger.info(f"🔍 Scanning {subnet}1–254 for Zenoh server on port {port}...")
            for i in range(1, 255):
                ip = f"{subnet}{i}"
                if is_zenoh_server(ip, port):
                    logger.info(f"✅ Found Zenoh server at {ip}:{port}")
                    return ip
            logger.error("❌ No Zenoh server found in subnet.")
            return None

        logger.info("Connecting to Zenoh network")
        try:
            if config is None:
                config = zenoh.Config()
                server_ip = find_zenoh_server("192.168.42.", 7447)
                if server_ip is None:
                    raise Exception("No Zenoh server found on the subnet.")
                    return False
                config.insert_json5("connect/endpoints", f'["tcp/{server_ip}:7447"]')

            self.zenoh_session = zenoh.open(config)

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

#     def connect_zenoh(self, config=None):
#         """Connect to Zenoh network"""
#         logger.info("Connecting to Zenoh network")
#         try:
#             import zenoh
# 
#             self.zenoh_session = zenoh.open(
#                 zenoh.Config() if config is None else config
#             )
# 
#             # Initialize publisher for data
#             self.data_publisher = self.zenoh_session.declare_publisher(
#                 f"measurement/node/{self.node_id}/data"
#             )
# 
#             # Initialize subscriber for heartbeat messages
#             self.heartbeat_subscriber = self.zenoh_session.declare_subscriber(
#                 "measurement/server/heartbeat", self._handle_heartbeat
#             )
# 
#             # Subscribe to control messages
#             self.control_subscriber = self.zenoh_session.declare_subscriber(
#                 f"measurement/node/{self.node_id}/control", self._handle_control_message
#             )
# 
#             # Subscribe to acknowledgment messages (for buffer publishing)
#             self.ack_subscriber = self.zenoh_session.declare_subscriber(
#                 f"measurement/server/ack/{self.node_id}", self._handle_server_ack
#             )
# 
#             logger.info(f"Node {self.node_id} connected to Zenoh")
# 
#             # Start publisher thread
#             self.publisher_thread = threading.Thread(
#                 target=self._publisher_worker, daemon=True
#             )
#             self.publisher_thread.start()
# 
#             # Start buffer processing thread
#             self.buffer_thread = threading.Thread(
#                 target=self._process_buffer, daemon=True
#             )
#             self.buffer_thread.start()
# 
#             # Start server checker thread
#             self.checker_thread = threading.Thread(
#                 target=self._check_server_status, daemon=True
#             )
#             self.checker_thread.start()
# 
#             # Check for initial server availability
#             self._check_server_connection()
#         except Exception as e:
#             logger.error(f"Failed to connect to Zenoh: {str(e)}")
#             raise

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

    @abc.abstractmethod
    def _update_environmental_data(self):
        """Update environmental data - to be implemented by child classes"""
        pass

    @abc.abstractmethod
    def _update_plate_readings(self):
        """Update plate readings - to be implemented by child classes"""
        pass

    def _prepare_data_payload(self):
        """Prepare measurement data payload"""
        timestamp = self._format_timestamp()

        payload = {
            "node_id": self.node_id,
            "timestamp": timestamp,
            "data": {
                "environment": {
                    "temperature": float(round(np.mean(self.env_data["temperature"]), 2)),
                    "humidity": float(round(np.mean(self.env_data["humidity"]), 2)),
                    "pressure": float(round(np.mean(self.env_data["pressure"]), 2)),
                },
                "plates": [],
            },
        }
        

        # Add plate data
        for plate in self.plates:
            plate_data = {
                "plate_id": plate["plate_id"],
                "target_voltage": plate["target_voltage"],
                "resistance": plate["resistance"],
                "reference_voltage": float(round(np.mean(plate["channels"][7]["voltage"]), 3)),
                "bias_voltage": float(round(plate["bias_voltage"], 3)),
                "channels": [],
            }

            # Add address if available (for hardware plates)
            if "address" in plate:
                plate_data["address"] = plate["address"]

            for idx, channel in enumerate(plate["channels"]):
                # skip the reference voltage channel
                if idx == 7:
                    continue;
                # note that energy accumulates on a per-second basis
                # whereas instantaneous power is an average over 10 seconds
                channel_data = {
                    "channel": idx,
                    "power_mW": float(round(np.mean(channel["power"]), 3)),
                    "energy_Wh": float(channel["energy"]),
                    "cell_id": channel["cell_id"],
                }

                # Add voltage if available (for hardware readings)
                if "voltage" in channel:
                    channel_data["voltage"] = float(round(np.mean(channel["voltage"]), 4))

                # Add timestamp if available (for hardware readings)
                if "timestamp" in channel:
                    channel_data["timestamp"] = str(channel["timestamp"][self.i - 1])  # self.i was incremented before this function

                plate_data["channels"].append(channel_data)

            payload["data"]["plates"].append(plate_data)
            
            #print(payload, flush=True)
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
                target_voltage = message.get("target_voltage")

                if plate_id and target_voltage is not None:
                    self._set_reference_voltage(plate_id, target_voltage)

                    # Send acknowledgment
                    if self.zenoh_session:
                        ack_publisher = self.zenoh_session.declare_publisher(
                            f"measurement/node/{self.node_id}/ack"
                        )
                        ack_publisher.put(
                            json.dumps(
                                {
                                    "action": "set_reference_voltage",
                                    "node_id": self.node_id,
                                    "plate_id": plate_id,
                                    "target_voltage": target_voltage,
                                    "status": "success",
                                    "timestamp": self._format_timestamp(),
                                }
                            )
                        )

            elif message.get("action") == "set_resistance":
                plate_id = message.get("plate_id")
                resistance = message.get("resistance")

                if plate_id and resistance is not None:
                    self._set_resistance(plate_id, resistance)

                    # Send acknowledgment
                    if self.zenoh_session:
                        ack_publisher = self.zenoh_session.declare_publisher(
                            f"measurement/node/{self.node_id}/ack"
                        )
                        ack_publisher.put(
                            json.dumps(
                                {
                                    "action": "set_resistance",
                                    "node_id": self.node_id,
                                    "plate_id": plate_id,
                                    "resistance": resistance,
                                    "status": "success",
                                    "timestamp": self._format_timestamp(),
                                }
                            )
                        )
                        
            elif message.get("action") == "set_calibration":
                plate_id = message.get("plate_id")
                channel = message.get("channel")
                target_voltage = message.get("target_voltage")
                cal_voltage = message.get("cal_voltage")

                if plate_id and channel is not None:
                    cal_resistance = self._set_calibration(plate_id, channel, target_voltage, cal_voltage)

                    # Send acknowledgment
                    if self.zenoh_session:
                        ack_publisher = self.zenoh_session.declare_publisher(
                            f"measurement/node/{self.node_id}/ack"
                        )
                        ack_publisher.put(
                            json.dumps(
                                {
                                    "action": "set_calibration",
                                    "node_id": self.node_id,
                                    "plate_id": plate_id,
                                    "channel": channel,
                                    "cal_resistance": cal_resistance,
                                    "status": "success",
                                    "timestamp": self._format_timestamp(),
                                }
                            )
                        )
                        
            elif message.get("action") == "assign_cell":
                plate_id = message.get("plate_id")
                channel = message.get("channel")
                cell_id = message.get("cell_id")
                energy = message.get("energy")

                if plate_id is not None and channel is not None and cell_id is not None:
                    self._assign_cell(plate_id, channel, cell_id, energy)

                    # Send acknowledgment
                    if self.zenoh_session:
                        ack_publisher = self.zenoh_session.declare_publisher(
                            f"measurement/node/{self.node_id}/ack"
                        )
                        ack_publisher.put(
                            json.dumps(
                                {
                                    "action": "assign_cell",
                                    "node_id": self.node_id,
                                    "plate_id": plate_id,
                                    "channel": channel,
                                    "cell_id": cell_id,
                                    "energy": energy,
                                    "status": "success",
                                    "timestamp": self._format_timestamp(),
                                }
                            )
                        )

        except json.JSONDecodeError:
            logger.error("Failed to parse control message")
        except Exception as e:
            logger.error(f"Error handling control message: {str(e)}")

    def _set_reference_voltage(self, plate_id, target_voltage):
        """Set reference voltage for a specific plate"""
        if not (0.5 <= target_voltage <= 2.0):
            logger.warning(f"Voltage {target_voltage}V out of acceptable range")
            return False

        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                logger.info(f"Setting reference voltage for {plate_id} to {target_voltage}V")
                plate["target_voltage"] = target_voltage
                return True

        logger.warning(f"Plate {plate_id} not found")
        return False

    def _set_resistance(self, plate_id, resistance):
        """Set resistance for a specific plate"""
        if not (0.0 <= resistance <= 100.0):
            logger.warning(f"Resistance {resistance}ohm out of acceptable range")
            return False

        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                logger.info(f"Setting resistance for {plate_id} to {resistance}ohm")
                plate["resistance"] = resistance
                return True

        logger.warning(f"Plate {plate_id} not found")
        return False
    
    def _set_calibration(self, plate_id, channel, target_voltage, cal_voltage):
        """Set calibration resistance for a specific plate and channel"""
        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                self.calibration_running = True
                cal_resistance = self._update_channel_calibration(plate_id, channel, target_voltage, cal_voltage)
                plate["channels"][channel]["cal_resistance"] = cal_resistance
                self.calibration_running = False
                return cal_resistance
        
        logger.warning(f"Plate {plate_id} not found")
        return False
    
    def _assign_cell(self, plate_id, channel, cell_id, energy):
        """Assign a cell ID to a specific channel"""
        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                if 0 <= channel < len(plate["channels"]):
                    logger.info(
                        f"Assigning cell {cell_id} to {plate_id} channel {channel}"
                    )
                    plate["channels"][channel]["cell_id"] = cell_id
                    plate["channels"][channel]["energy"] = energy
                    return True
                else:
                    logger.warning(f"Invalid channel number {channel}")
                    return False

        logger.warning(f"Plate {plate_id} not found")
        return False

    def _format_timestamp(self):
        """Get current time formatted as YYYY-MM-DD HH:MM:SS:MS +ZZZZ"""
        return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S:%f %z")

    def load_last_session (self):
        import requests

    def load_last_session(self):
        logger.info("🔄 Loading last session state from server via Zenoh...")

        for plate in self.plates:
            plate_id = plate["plate_id"]

            for channel_idx, channel in enumerate(plate["channels"]):
                if channel_idx == 7:
                    continue
                key = f"session/last/{self.node_id}/{plate_id}/{channel_idx}"
                try:
                    replies = self.zenoh_session.get(key, timeout=1500)  # timeout in ms

                    for reply in replies:
                        if reply.ok and reply.result:
                            payload_bytes = bytes(reply.result.payload)
                            payload = json.loads(payload_bytes.decode("utf-8"))

                            channel["cell_id"] = payload.get("cell_id")
                            channel["energy"] = float(payload.get("energy_Wh", 0.0))
                            channel["cal_resistance"] = payload.get(f"ch{channel_idx}_cal_resistance")

                            if channel_idx == 0:
                                plate["target_voltage"] = float(payload.get("target_voltage", 1.2))
                                plate["resistance"] = float(payload.get("resistance", 22.0))
                                plate["bias_voltage"] = float(payload.get("bias_voltage", 0.5))

                            logger.info(f"🧠 Restored {plate_id}/ch{channel_idx}: {payload}")
                        else:
                            logger.warning(f"⚠️ Invalid or empty reply for {plate_id}/ch{channel_idx}")

    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to load session for {plate_id}/ch{channel_idx}: {str(e)}")
            
    def run(self):
        """Main operation loop"""
        logger.info(f"Starting node {self.node_id} with {len(self.plates)} plates")

        try:
            while not self.stopping:
                # Update data (order matters)
                if not self.calibration_running:
                    self._update_environmental_data()
                    self._update_plate_readings()
                    
                    self.i += 1
                    # Reset index to 0 after full cycle
                    if self.i == self.array_size:
                        # Create payload and put in the publishing queue
                        payload, _ = self._prepare_data_payload()
                        self.publish_queue.put((payload, False))
                        self._update_reference_voltage()
                        self.i = 0

                # Wait for next second
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Node stopped by user")
        except Exception as e:
            logger.error(f"Error in node operation: {str(e)}")
        finally:
            self.close()
