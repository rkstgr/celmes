#!/usr/bin/env python3
"""
Server implementation for the measurement system using Zenoh and TimescaleDB
"""

import os
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import zenoh
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from contextlib import asynccontextmanager
import threading  # Add this import at the top with other imports

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MeasurementServer")

# Database connection parameters
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASSWORD")

if not DB_HOST:
    raise ValueError("DB_HOST environment variable is not set")

if not DB_PORT:
    raise ValueError("DB_PORT environment variable is not set")

if not DB_NAME:
    raise ValueError("DB_NAME environment variable is not set")

if not DB_USER:
    raise ValueError("DB_USER environment variable is not set")

if not DB_PASS:
    raise ValueError("DB_PASSWORD environment variable is not set")

UNCONFIGURED = "unconfigured"

class NodeConfig(BaseModel):
    node_id: str
    name: Optional[str] = None
    location: Optional[str] = None
    properties: Dict[str, Any] = {}


class SetReferenceVoltage(BaseModel):
    node_id: str
    plate_id: str
    value: float


class DataCollector:
    """Collects data from nodes via Zenoh and stores it in TimescaleDB"""

    def __init__(self, db_params: Dict[str, Any]):
        self.db_params = db_params
        self.zenoh_session = None
        self.running = False
        self.nodes = {}  # Track known nodes
        self.db_conn = None  # Persistent database connection

    def connect_db(self):
        """Connect to TimescaleDB if not already connected"""
        if self.db_conn and not self.db_conn.closed:
            return self.db_conn

        try:
            logger.info(
                f"Server starting with DB {self.db_params.get('dbname')}@{self.db_params.get('host')}:{self.db_params.get('port')}"
            )
            self.db_conn = psycopg2.connect(
                host=self.db_params.get("host", "localhost"),
                port=self.db_params.get("port", 5432),
                dbname=self.db_params.get("dbname", "measurement_db"),
                user=self.db_params.get("user", "postgres"),
                password=self.db_params.get("password", "password"),
            )
            return self.db_conn
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    def close_db(self):
        """Close database connection"""
        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None
            logger.info("Database connection closed")

    def setup_db(self):
        """Set up database tables and TimescaleDB hypertables"""
        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            # Create tables if they don't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS node (
                node_id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT,
                properties JSONB,
                last_seen TIMESTAMP WITH TIME ZONE
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS cell (
                cell_id TEXT PRIMARY KEY,
                volume FLOAT,
                properties JSONB,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS environmental_data (
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                node_id TEXT NOT NULL,
                temperature FLOAT,
                humidity FLOAT,
                pressure FLOAT,
                FOREIGN KEY (node_id) REFERENCES node (node_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS plate_data (
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                node_id TEXT NOT NULL,
                plate_id TEXT NOT NULL,
                reference_voltage FLOAT,
                bias_voltage FLOAT,
                FOREIGN KEY (node_id) REFERENCES node (node_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS plate (
                node_id TEXT NOT NULL,
                plate_id TEXT NOT NULL,
                target_voltage FLOAT,
                resistance FLOAT DEFAULT 22.0,
                ch0_cal_resistance FLOAT DEFAULT 0.0,
                ch1_cal_resistance FLOAT DEFAULT 0.0,
                ch2_cal_resistance FLOAT DEFAULT 0.0,
                ch3_cal_resistance FLOAT DEFAULT 0.0,
                ch4_cal_resistance FLOAT DEFAULT 0.0,
                ch5_cal_resistance FLOAT DEFAULT 0.0,
                ch6_cal_resistance FLOAT DEFAULT 0.0,
                PRIMARY KEY (node_id, plate_id),
                FOREIGN KEY (node_id) REFERENCES node (node_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_data (
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                node_id TEXT NOT NULL,
                plate_id TEXT NOT NULL,
                channel INTEGER NOT NULL,
                cell_id TEXT NOT NULL,
                power_mW FLOAT,
                energy_Wh FLOAT,
                FOREIGN KEY (node_id) REFERENCES node (node_id),
                FOREIGN KEY (cell_id) REFERENCES cell (cell_id)
            )
            """)

            # Ensure unconfigured cell exists
            cursor.execute("""
                INSERT INTO cell (cell_id, properties)
                VALUES ('unconfigured', '{"type": "system", "description": "Default virtual cell for unassigned channels"}')
                ON CONFLICT (cell_id) DO NOTHING
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS path_cell_mapping (
                id SERIAL PRIMARY KEY,
                node_id TEXT NOT NULL,
                plate_id TEXT NOT NULL,
                channel INTEGER NOT NULL,
                cell_id TEXT NOT NULL,
                start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                end_time TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY (node_id) REFERENCES node (node_id),
                FOREIGN KEY (cell_id) REFERENCES cell (cell_id)
            )
            """)

            # Convert to TimescaleDB hypertables
            # Check if the tables are already hypertables
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
            if cursor.fetchone():
                try:
                    cursor.execute(
                        "SELECT create_hypertable('environmental_data', 'time', if_not_exists => TRUE)"
                    )
                    cursor.execute(
                        "SELECT create_hypertable('plate_data', 'time', if_not_exists => TRUE)"
                    )
                    cursor.execute(
                        "SELECT create_hypertable('channel_data', 'time', if_not_exists => TRUE)"
                    )
                except psycopg2.Error as e:
                    logger.warning(
                        f"Error creating hypertables (may already exist): {str(e)}"
                    )

            # Commit changes
            conn.commit()
            logger.info("Database tables initialized")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error setting up database: {str(e)}")

    def connect_zenoh(self, config=None):
        """Connect to Zenoh network"""
        logger.info("Connecting to Zenoh network")

        try:
            # Create Zenoh config
            zenoh_config = zenoh.Config() if config is None else config
            zenoh_config.insert_json5("listen/endpoints", '["tcp/0.0.0.0:7447"]')

            # Open Zenoh session (non-async)
            self.zenoh_session = zenoh.open(zenoh_config)

        except Exception as e:
            logger.error(f"Failed to connect to Zenoh network: {str(e)}")
            raise

    def start_collecting(self):
        """Start collecting data from nodes - enhanced for buffering support"""
        if self.running:
            return

        self.running = True
        try:
            # Subscribe to regular data
            data_key_expr = "measurement/node/*/data"
            self.subscriber = self.zenoh_session.declare_subscriber(
                data_key_expr, self._handle_data
            )

            # Subscribe to buffered data
            buffered_data_key_expr = "measurement/node/*/buffered_data"
            self.buffered_subscriber = self.zenoh_session.declare_subscriber(
                buffered_data_key_expr, self._handle_buffered_data
            )

            # Subscribe to buffer batch metadata
            batch_key_expr = "measurement/node/*/buffer_batch"
            self.batch_subscriber = self.zenoh_session.declare_subscriber(
                batch_key_expr, self._handle_buffer_batch
            )

            # Subscribe to node pings
            ping_key_expr = "measurement/node/*/ping"
            self.ping_subscriber = self.zenoh_session.declare_subscriber(
                ping_key_expr, self._handle_ping
            )

            # Initialize heartbeat publisher
            self.heartbeat_publisher = self.zenoh_session.declare_publisher(
                "measurement/server/heartbeat"
            )

            # Start heartbeat thread
            self.heartbeat_thread = threading.Thread(
                target=self._send_heartbeats, daemon=True
            )
            self.heartbeat_thread.start()

            # Regular discovery queryable
            self.discovery_queryable = self.zenoh_session.declare_queryable(
                "measurement/discovery", self._handle_discovery_query
            )

            # Session state queryable (for node startup state)
            self.session_queryable = self.zenoh_session.declare_queryable(
                "session/last/**",  # Handles node/plate/channel queries
                self._handle_session_query
            )

            # Subscribe to calibration_done acknowledgments from nodes
            self.ack_subscriber = self.zenoh_session.declare_subscriber(
                "measurement/node/*/ack",
                self._handle_ack
            )

            logger.info("Data collection started - Zenoh ready")

        except Exception as e:
            self.running = False
            logger.error(f"Failed to start collection: {str(e)}")
            raise

    def stop_collecting(self):
        """Stop collecting data"""
        if not self.running:
            return

        self.running = False

        # Stop heartbeat thread
        if hasattr(self, "heartbeat_thread"):
            self.heartbeat_thread.join(timeout=2)

        # Undeclare all Zenoh resources
        try:
            if hasattr(self, "subscriber"):
                self.subscriber.undeclare()
            if hasattr(self, "buffered_subscriber"):
                self.buffered_subscriber.undeclare()
            if hasattr(self, "batch_subscriber"):
                self.batch_subscriber.undeclare()
            if hasattr(self, "ping_subscriber"):
                self.ping_subscriber.undeclare()
            if hasattr(self, "heartbeat_publisher"):
                self.heartbeat_publisher.undeclare()
            if hasattr(self, "discovery_queryable"):
                self.discovery_queryable.undeclare()
            if hasattr(self, "session_queryable"):
                self.session_queryable.undeclare()
            if hasattr(self, "ack_subscriber"):
                self.ack_subscriber.undeclare()
        except Exception as e:
            logger.warning(f"⚠️ Error during Zenoh resource cleanup: {e}")

        if self.zenoh_session:
            self.zenoh_session.close()
            logger.info("Zenoh session closed")

        self.close_db()  # Close database connection when stopping

    def _send_heartbeats(self):
        """Send periodic heartbeats to let nodes know the server is online"""
        while self.running:
            try:
                self.heartbeat_publisher.put(
                    json.dumps({"timestamp": int(time.time()), "status": "online"})
                )
                time.sleep(1)  # Send heartbeat every second
            except Exception as e:
                logger.error(f"Error sending heartbeat: {str(e)}")
                time.sleep(5)  # Back off on error

    def _handle_ping(self, sample):
        """Handle ping messages from nodes"""
        try:
            message = json.loads(sample.payload.to_string())
            node_id = message.get("node_id")

            # Immediately respond with a targeted heartbeat
            if node_id:
                ack_publisher = self.zenoh_session.declare_publisher(
                    "measurement/server/heartbeat"
                )
                ack_publisher.put(
                    json.dumps(
                        {
                            "timestamp": int(time.time()),
                            "status": "online",
                            "response_to": node_id,
                        }
                    )
                )
                logger.debug(f"Responded to ping from node {node_id}")
        except Exception as e:
            logger.error(f"Error handling ping: {str(e)}")

    def _handle_buffered_data(self, sample):
        """Process incoming buffered data from nodes"""
        # Use the same processing logic as regular data
        self._handle_data(sample)
        logger.debug("Processed buffered data")

    def _handle_buffer_batch(self, sample):
        """Handle buffer batch metadata and send acknowledgments"""
        try:
            message = json.loads(sample.payload.to_string())
            node_id = message.get("node_id")
            batch_id = message.get("batch_id")
            count = message.get("count", 0)

            if node_id and batch_id:
                # Send acknowledgment back to the node
                ack_publisher = self.zenoh_session.declare_publisher(
                    f"measurement/server/ack/{node_id}"
                )
                ack_publisher.put(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "success": True,
                            "timestamp": int(time.time()),
                        }
                    )
                )
                logger.info(
                    f"Acknowledged batch of {count} measurements from node {node_id}"
                )
        except Exception as e:
            logger.error(f"Error handling buffer batch: {str(e)}")

    def _handle_data(self, sample):
        """Process incoming data from nodes"""
        try:
            payload = json.loads(sample.payload.to_string())
            node_id = payload.get("node_id")
            timestamp = payload.get("timestamp")
            data = payload.get("data", {})

            if not node_id or not timestamp or not data:
                logger.warning(f"Invalid data payload from {node_id}")
                return

            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S:%f %z")
            self._ensure_node_exists(node_id)

            if "environment" in data:
                env = data["environment"]
                logger.info(
                    f"Node {node_id}: env({env.get('temperature'):.1f}°C, {env.get('humidity'):.1f}%, {env.get('pressure'):.1f}hPa)"
                )
                self._store_environmental_data(node_id, dt, env)

            if "plates" in data:
                total_channels = sum(len(p.get("channels", [])) for p in data["plates"])
                self._store_plate_data(node_id, dt, data["plates"])
                logger.info(
                    f"Node {node_id}: stored {total_channels} channels across {len(data['plates'])} plates"
                )

            self._update_node_last_seen(node_id, dt)

        except json.JSONDecodeError:
            logger.error("Invalid JSON payload")
        except Exception as e:
            logger.error(f"Data processing failed: {str(e)}")

    def _handle_discovery_query(self, query):
        """Handle discovery queries"""
        try:
            nodes_list = list(self.nodes.values())
            query.reply(
                json.dumps(
                    {
                        "nodes": nodes_list,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %z"),
                    }
                )
            )
            logger.debug(f"Discovery query: returned {len(nodes_list)} nodes")
        except Exception as e:
            logger.error(f"Discovery query failed: {str(e)}")

    def _handle_session_query(self, query):
        try:
            # Parse the resource path: session/last/<node_id>/<plate_id>/<channel>
            parts = str(query.key_expr).split("/")
            if len(parts) < 5:
                logger.warning(f"Malformed session query key: {query.key_expr}")
                return

            _, _, node_id, plate_id, channel_str = parts
            channel = int(channel_str)

            # Default response
            session_data = {
                "cell_id": None,
                "energy_Wh": 0.0,
                "target_voltage": 1.2,
                "resistance": 22.0,
                "bias_voltage": 0.5,
                "ch0_cal_resistance": 0.0,
                "ch1_cal_resistance": 0.0,
                "ch2_cal_resistance": 0.0,
                "ch3_cal_resistance": 0.0,
                "ch4_cal_resistance": 0.0,
                "ch5_cal_resistance": 0.0,
                "ch6_cal_resistance": 0.0,
            }

            cursor = self.connect_db().cursor()

            # Get current cell_id for this path
            cursor.execute(
                """
                SELECT cell_id FROM path_cell_mapping
                WHERE node_id = %s AND plate_id = %s AND channel = %s
                AND end_time IS NULL
                ORDER BY start_time DESC LIMIT 1
                """,
                (node_id, plate_id, channel)
            )
            row = cursor.fetchone()
            if row:
                session_data["cell_id"] = row[0]

            # Get last energy_Wh from channel_data
            if session_data["cell_id"]:
                cursor.execute(
                    """
                    SELECT energy_Wh FROM channel_data
                    WHERE cell_id = %s
                    ORDER BY time DESC
                    LIMIT 1
                    """,
                    (session_data["cell_id"],)
                )
                row = cursor.fetchone()
                if row:
                    session_data["energy_Wh"] = float(row[0])

            # ✅ Get target_voltage from the plate table
            cursor.execute(
                """
                SELECT target_voltage FROM plate
                WHERE node_id = %s AND plate_id = %s
                LIMIT 1
                """,
                (node_id, plate_id)
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                session_data["target_voltage"] = float(row[0])

            # ✅ Get resistance and chX_cal_resistances from the plate table
            cursor.execute(
                """
                SELECT resistance,
                       ch0_cal_resistance, ch1_cal_resistance, ch2_cal_resistance,
                       ch3_cal_resistance, ch4_cal_resistance, ch5_cal_resistance, ch6_cal_resistance
                FROM plate
                WHERE node_id = %s AND plate_id = %s
                LIMIT 1
                """,
                (node_id, plate_id)
            )
            row = cursor.fetchone()
            if row:
                if row[0] is not None:
                    session_data["resistance"] = float(row[0])
                for i in range(7):
                    col_value = row[i + 1]  # row[1] to row[7]
                    if col_value is not None:
                        session_data[f"ch{i}_cal_resistance"] = float(col_value)

            # ✅ Get latest bias_voltage from the plate_data time series
            cursor.execute(
                """
                SELECT bias_voltage FROM plate_data
                WHERE node_id = %s AND plate_id = %s
                ORDER BY time DESC
                LIMIT 1
                """,
                (node_id, plate_id)
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                session_data["bias_voltage"] = float(row[0])

            cursor.close()

            try:
                payload = json.dumps(session_data)
                logger.debug(f"Replying to query {query.key_expr} with: {payload}")
                query.reply(
                    key_expr=query.key_expr,
                    payload=payload
                )
                logger.info(f"🧠 Responded to session query for {node_id}/{plate_id}/ch{channel}: {session_data}")
            except Exception as e:
                logger.error(f"❌ Failed to reply to session query: {e}")

        except Exception as e:
            logger.error(f"Failed to handle session query {query.key_expr}: {str(e)}")

    def _handle_ack(self, sample):
        """Handle acknowledgment messages from node and update respective database tables"""
        try:
            message = json.loads(sample.payload.to_string())

            action = message.get("action")
            node_id = message.get("node_id")
            plate_id = message.get("plate_id")

            if action == "set_calibration":
                channel = message.get("channel")
                cal_resistance = message.get("cal_resistance")

                if not all([node_id, plate_id, channel is not None, cal_resistance is not None]):
                    logger.warning("⚠️ Incomplete calibration message, ignoring.")
                    return

                column_name = f"ch{channel}_cal_resistance"
                conn = self.connect_db()
                cursor = conn.cursor()

                sql = f"""
                    INSERT INTO plate (node_id, plate_id, {column_name})
                    VALUES (%s, %s, %s)
                    ON CONFLICT (node_id, plate_id)
                    DO UPDATE SET {column_name} = EXCLUDED.{column_name}
                """
                cursor.execute(sql, (node_id, plate_id, cal_resistance))
                conn.commit()
                cursor.close()

                logger.info(f"✅ Calibration complete: Updated {node_id}/{plate_id}/ch-{channel} to {cal_resistance} Ω")
            else:
                logger.info(f"✅ Got Acknowledgment Message from {node_id}/{plate_id} for {action}")

        except Exception as e:
            logger.error(f"❌ Failed to handle calibration_done message: {e}")

    def _ensure_node_exists(self, node_id):
        """Make sure the node exists in the database"""
        if node_id in self.nodes:
            return

        try:
            cursor = self.connect_db().cursor()
            # Check if node exists
            cursor.execute("SELECT 1 FROM node WHERE node_id = %s", (node_id,))
            if not cursor.fetchone():
                # Insert new node
                cursor.execute(
                    "INSERT INTO node (node_id, last_seen) VALUES (%s, NOW())",
                    (node_id,),
                )
                self.db_conn.commit()
                logger.info(f"Added new node: {node_id}")

            # Add to in-memory cache
            self.nodes[node_id] = {
                "node_id": node_id,
                "last_seen": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error(f"Error ensuring node exists: {str(e)}")
            if isinstance(e, psycopg2.Error):
                self.close_db()
        finally:
            cursor.close()  # Only close cursor

    def _update_node_last_seen(self, node_id, timestamp):
        """Update node's last seen timestamp"""
        try:
            cursor = self.connect_db().cursor()
            cursor.execute(
                "UPDATE node SET last_seen = %s WHERE node_id = %s",
                (timestamp, node_id),
            )
            self.db_conn.commit()
            # Update in-memory cache
            if node_id in self.nodes:
                self.nodes[node_id]["last_seen"] = timestamp.isoformat()
        except Exception as e:
            logger.error(f"Error updating node last seen: {str(e)}")
            if isinstance(e, psycopg2.Error):
                self.close_db()
        finally:
            cursor.close()  # Only close cursor

    def _store_environmental_data(self, node_id, timestamp, env_data):
        """Store environmental data in TimescaleDB"""
        try:
            cursor = self.connect_db().cursor()

            temperature = env_data.get("temperature")
            humidity = env_data.get("humidity")
            pressure = env_data.get("pressure")
            if temperature == 0.0 and humidity == 0.0 and pressure == 0.0:
                logger.info(
                    f"Env Sensor not connected to {node_id}, Env data not saved"
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO environmental_data
                    (time, node_id, temperature, humidity, pressure)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        timestamp,
                        node_id,
                        temperature,
                        humidity,
                        pressure,
                    ),
                )

                self.db_conn.commit()

        except Exception as e:
            logger.error(f"Error storing environmental data: {str(e)}")
            if isinstance(e, psycopg2.Error):
                self.close_db()  # Only close connection on database errors


    def _store_plate_data(self, node_id, timestamp, plates_data):
        """Store plate and channel data in TimescaleDB"""
        try:
            cursor = self.connect_db().cursor()

            for plate in plates_data:
                plate_id = plate.get("plate_id")
                ref_voltage = plate.get("reference_voltage")
                bias_voltage = plate.get("bias_voltage")

                if not plate_id:
                    logger.warning("Skipping plate with missing plate_id")
                    continue

                # Store plate data
                cursor.execute(
                    """
                    INSERT INTO plate_data
                    (time, node_id, plate_id, reference_voltage, bias_voltage)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (timestamp, node_id, plate_id, ref_voltage, bias_voltage),
                )

                # Store channel data
                channels = plate.get("channels", [])
                for channel_data in channels:
                    channel = channel_data.get("channel")
                    power_mW = channel_data.get("power_mW")
                    energy_Wh = channel_data.get("energy_Wh")

                    # Get the current mapped cell_id for this path
                    current_cell_id = self.get_cell_for_path(
                        node_id, plate_id, channel, timestamp, cursor
                    )

                    if current_cell_id == UNCONFIGURED:
                        # skip and do not persist channel data
                        continue

                    # Store the measurement with the correct cell_id
                    cursor.execute(
                        """
                        INSERT INTO channel_data
                        (time, node_id, plate_id, channel, cell_id, power_mW, energy_Wh)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            timestamp,
                            node_id,
                            plate_id,
                            channel,
                            current_cell_id,
                            power_mW,
                            energy_Wh,
                        ),
                    )

            self.db_conn.commit()

        except Exception as e:
            logger.error(f"Plate data storage failed for {node_id}: {str(e)}")
            if isinstance(e, psycopg2.Error):
                self.close_db()
            raise
        finally:
            cursor.close()


    def get_cell_for_path(self, node_id, plate_id, channel, timestamp, cursor):
        """Get the cell_id for a path at a specific time using existing cursor"""
        try:
            cursor.execute(
                """
                SELECT cell_id FROM path_cell_mapping
                WHERE node_id = %s AND plate_id = %s AND channel = %s
                AND start_time <= %s AND (end_time IS NULL OR end_time > %s)
                ORDER BY start_time DESC LIMIT 1
                """,
                (node_id, plate_id, channel, timestamp, timestamp),
            )

            result = cursor.fetchone()
            return result[0] if result else UNCONFIGURED
        except Exception as e:
            logger.error(f"Error getting cell for path: {str(e)}")
            return UNCONFIGURED
        # Don't close cursor here since it's passed in

    def reassign_path_to_cell(self, node_id, plate_id, channel, new_cell_id):
        """Reassign a path to a new cell"""
        try:
            now = datetime.now(timezone.utc)
            cursor = self.connect_db().cursor()

            # End current mapping and create new one
            cursor.execute(
                "UPDATE path_cell_mapping SET end_time = %s WHERE node_id = %s AND plate_id = %s AND channel = %s AND end_time IS NULL",
                (now, node_id, plate_id, channel),
            )
            cursor.execute(
                "INSERT INTO path_cell_mapping (node_id, plate_id, channel, cell_id, start_time) VALUES (%s, %s, %s, %s, %s)",
                (node_id, plate_id, channel, new_cell_id, now),
            )
            self.db_conn.commit()
            logger.info(
                f"🔄 Updating path_cell_mapping: reassigned {node_id}/{plate_id}/ch{channel} -> {new_cell_id}"
            )

        finally:
            cursor.close()

    def get_nodes(self):
        """Get list of all nodes from database"""
        try:
            conn = self.connect_db()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cursor.execute(
                """
                SELECT node_id, name, location, properties, last_seen
                FROM node
                ORDER BY last_seen DESC
                """
            )

            nodes = []
            for row in cursor.fetchall():
                node = dict(row)
                # Convert last_seen to string if it's a datetime
                if isinstance(node["last_seen"], datetime):
                    node["last_seen"] = node["last_seen"].isoformat()
                nodes.append(node)

            return nodes

        except Exception as e:
            logger.error(f"Error getting nodes: {str(e)}")
            return []
        finally:
            cursor.close()  # Only close cursor

    def update_node_config(self, node_config: NodeConfig):
        """Update node configuration"""
        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE node
                SET name = %s, location = %s, properties = %s
                WHERE node_id = %s
                """,
                (
                    node_config.name,
                    node_config.location,
                    json.dumps(node_config.properties),
                    node_config.node_id,
                ),
            )

            if cursor.rowcount == 0:
                # Node doesn't exist yet
                cursor.execute(
                    """
                    INSERT INTO node (node_id, name, location, properties, last_seen)
                    VALUES (%s, %s, %s, %s, NOW())
                    """,
                    (
                        node_config.node_id,
                        node_config.name,
                        node_config.location,
                        json.dumps(node_config.properties),
                    ),
                )

            conn.commit()

            # Update in-memory cache
            self.nodes[node_config.node_id] = {
                "node_id": node_config.node_id,
                "name": node_config.name,
                "location": node_config.location,
                "properties": node_config.properties,
                "last_seen": datetime.now().isoformat(),
            }

            return True

        except Exception as e:
            logger.error(f"Error updating node config: {str(e)}")
            return False
        finally:
            cursor.close()  # Only close cursor

    def send_control_message(self, node_id: str, action: str, **params):
        """Send control message to node"""
        if not self.zenoh_session:
            logger.error("No Zenoh session")
            return False

        try:
            message = {"action": action, "timestamp": int(time.time()), **params}
            publisher = self.zenoh_session.declare_publisher(
                f"measurement/node/{node_id}/control"
            )
            publisher.put(json.dumps(message))
            logger.info(f"Node {node_id}: sent {action}")
            return True
        except Exception as e:
            logger.error(f"Control message failed: {str(e)}")
            return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup services"""
    global collector

    # Startup
    collector = DataCollector(
        {
            "host": DB_HOST,
            "port": DB_PORT,
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASS,
        }
    )
    collector.setup_db()

    # Connect to Zenoh and start collecting (now synchronous)
    try:
        collector.connect_zenoh()
        collector.start_collecting()
        logger.info("Data collector initialized and started")
    except Exception as e:
        logger.error(f"Failed to initialize data collector: {str(e)}")
        collector.close_db()  # Ensure DB connection is closed if startup fails

    yield

    # Shutdown
    if collector:
        collector.stop_collecting()  # This will close both Zenoh and DB connections


# Create FastAPI app with lifespan
app = FastAPI(title="Measurement System API", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize data collector
collector = None


# API endpoints
@app.get("/api/nodes", response_model=List[Dict[str, Any]])
async def get_nodes():
    """Get list of all nodes"""
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    return collector.get_nodes()


@app.put("/api/nodes/{node_id}")
async def update_node(node_config: NodeConfig):
    """Update node configuration"""
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    success = collector.update_node_config(node_config)

    if not success:
        raise HTTPException(
            status_code=500, detail="Failed to update node configuration"
        )

    return {"status": "success"}

@app.get("/api/data/environmental")
async def get_environmental_data(
    node_id: str, start_time: str = None, end_time: str = None, limit: int = 1000
):
    """Get environmental data for a node"""
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    try:
        conn = collector.connect_db()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
        SELECT time, temperature, humidity, pressure
        FROM environmental_data
        WHERE node_id = %s
        """
        params = [node_id]

        if start_time:
            query += " AND time >= %s"
            params.append(start_time)

        if end_time:
            query += " AND time <= %s"
            params.append(end_time)

        query += " ORDER BY time DESC LIMIT %s"
        params.append(limit)

        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            data = dict(row)
            # Convert time to string
            if isinstance(data["time"], datetime):
                data["time"] = data["time"].isoformat()
            results.append(data)

        return results

    except Exception as e:
        logger.error(f"Error querying environmental data: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query error")


@app.get("/api/data/power")
async def get_power_data(
    node_id: str,
    plate_id: str = None,
    channel: int = None,
    start_time: str = None,
    end_time: str = None,
    limit: int = 1000,
):
    """Get power data for a node/plate/channel"""
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    try:
        conn = collector.connect_db()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
        SELECT time, plate_id, channel, power_mW, energy_Wh
        FROM channel_data
        WHERE node_id = %s
        """
        params = [node_id]

        if plate_id:
            query += " AND plate_id = %s"
            params.append(plate_id)

        if channel is not None:
            query += " AND channel = %s"
            params.append(channel)

        if start_time:
            query += " AND time >= %s"
            params.append(start_time)

        if end_time:
            query += " AND time <= %s"
            params.append(end_time)

        query += " ORDER BY time DESC LIMIT %s"
        params.append(limit)

        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            data = dict(row)
            # Convert time to string
            if isinstance(data["time"], datetime):
                data["time"] = data["time"].isoformat()
            results.append(data)

        return results

    except Exception as e:
        logger.error(f"Error querying power data: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query error")



@app.get("/api/data/cell-mapping")
async def get_cell_mapping(
    node_id: str,
    plate_id: str,
    channel: int
):
    """Get current cell mapping for node/plate/channel"""
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    conn = collector.connect_db()
    cursor = conn.cursor()
    timestamp = datetime.now().astimezone()
    current_cell = collector.get_cell_for_path(node_id, plate_id, channel, timestamp, cursor)
    print(timestamp, current_cell)
    return current_cell

@app.post("/api/control/assign-cell")
async def assign_cell_control(message: Dict[str, Any]):
    """
    Send an 'assign_cell' control message to a specific node.
    Expected JSON:
    {
        "node_id": "node-abc123",
        "plate_id": "plate_1",
        "channel": 2,
        "cell_id": "abc123"
    }
    """
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    try:
        node_id = message["node_id"]
        plate_id = message["plate_id"]
        channel = message["channel"]
        cell_id = message["cell_id"]
        volume = message["volume"]
        energy_override = message.get("energy_wh", None)

        if energy_override is not None:
            latest_energy = float(energy_override)
            logger.info(
                    f"Forwarding user defined energy ⚡ to node️: {latest_energy}Wh "
                )
        else:
            # 🔍 Check for latest energy_Wh for the cell_id
            try:
                conn = collector.connect_db()
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT energy_Wh
                    FROM channel_data
                    WHERE cell_id = %s
                    ORDER BY time DESC
                    LIMIT 1
                    """,
                    (cell_id,)
                )
                row = cursor.fetchone()
                if row:
                    latest_energy = float(row[0])
                    logger.info(f"✅ Found last reported energy ⚡️ for cell '{cell_id}': {latest_energy} Wh")
                else:
                    latest_energy = 0.0
                    logger.info(f"ℹ️ No prior energy found for cell '{cell_id}'. Starting fresh at 0.0 Wh")
                cursor.close()
            except Exception as db_err:
                logging.warning(f"⚠️ Could not retrieve energy for cell '{cell_id}': {db_err}")
                latest_energy = 0.0

        # 🔐 Ensure the cell_id exists in the cell table
        try:
            conn = collector.connect_db()
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO cell (cell_id, volume, properties)
                VALUES (%s, %s, %s)
                ON CONFLICT (cell_id) DO NOTHING
                RETURNING cell_id
                """,
                (cell_id, volume, json.dumps({"source": "auto-created by node"})),
            )
            inserted = cursor.fetchone()

            if inserted:
                logger.info(f"➕ Added new cell_id: '{cell_id}' with volume={volume}")
            else:
                cursor.execute(
                    """
                    UPDATE cell
                    SET volume = %s,
                        updated_at = NOW()
                    WHERE cell_id = %s
                    """,
                    (volume, cell_id),
                )
                logger.info(f"✅ Updated cell_id: '{cell_id}' with new volume={volume}")

        except Exception as db_err:
            logging.warning(f"⚠️ Could not set cell table '{cell_id}': {db_err}")

        # 📦 Send full control message to the node
        success = collector.send_control_message(
            node_id,
            action="assign_cell",
            plate_id=plate_id,
            channel=channel,
            cell_id=cell_id,
            energy=latest_energy,
        )

        if success:
            collector.reassign_path_to_cell(node_id, plate_id, channel, cell_id)
        if not success:
            raise HTTPException(
                status_code=500, detail="Failed to send assign_cell command"
            )

        return {
            "status": "success",
            "sent_to": node_id,
            "cell_id": cell_id,
            "energy": latest_energy,
        }

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing field: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.post("/api/control/set-reference-voltage")
async def set_reference_voltage_control(message: Dict[str, Any]):
    """
    Send a 'set_reference_voltage' control message to a specific node.
    Also updates the target_voltage in the plate_config table.
    Expected JSON:
    {
        "node_id": "node-abc123",
        "plate_id": "plate_1",
        "target_voltage": 1.20
    }
    """
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    try:
        node_id = message["node_id"]
        plate_id = message["plate_id"]
        target_voltage = float(message["target_voltage"])

        if not (0.0 <= target_voltage <= 4.095):
            raise HTTPException(
                status_code=400,
                detail="Voltage must be between 1.0V and 5.0V"
            )

        # ✅ Update the target voltage in plate table
        try:
            conn = collector.connect_db()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO plate (node_id, plate_id, target_voltage)
                VALUES (%s, %s, %s)
                ON CONFLICT (node_id, plate_id)
                DO UPDATE SET target_voltage = EXCLUDED.target_voltage
            """, (node_id, plate_id, target_voltage))
            conn.commit()
            cursor.close()
            logger.info(f"💾 Updated target_voltage for {node_id}/{plate_id} to {target_voltage}V")
        except Exception as db_err:
            logger.warning(f"⚠️ Failed to update plate table: {db_err}")

        # ✅ Send the control message to the node
        success = collector.send_control_message(
            node_id,
            action="set_reference_voltage",
            plate_id=plate_id,
            target_voltage=target_voltage
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to send set_reference_voltage command"
            )

        return {"status": "success", "sent_to": node_id, "voltage": target_voltage}

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing field: {str(e)}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Voltage must be a float")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.post("/api/control/set-resistance")
async def set_resistance_control(message: Dict[str, Any]):
    """
    Send a 'set_resistance' control message to a specific node.
    Also updates the resistance in the plate table.
    Expected JSON:
    {
        "node_id": "node-abc123",
        "plate_id": "plate_1",
        "resistance": 1.20
    }
    """
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    try:
        node_id = message["node_id"]
        plate_id = message["plate_id"]
        resistance = float(message["resistance"])

        if not (0.0 <= resistance <= 100):
            raise HTTPException(
                status_code=400,
                detail="Resistor value must be between 0ohm and 100ohm"
            )

        # ✅ Update the resistance in plate table
        try:
            conn = collector.connect_db()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO plate (node_id, plate_id, resistance)
                VALUES (%s, %s, %s)
                ON CONFLICT (node_id, plate_id)
                DO UPDATE SET resistance = EXCLUDED.resistance
            """, (node_id, plate_id, resistance))
            conn.commit()
            cursor.close()
            logger.info(f"💾 Updated resistance for {node_id}/{plate_id} to {resistance}ohm")
        except Exception as db_err:
            logger.warning(f"⚠️ Failed to update plate table: {db_err}")

        # ✅ Send the control message to the node
        success = collector.send_control_message(
            node_id,
            action="set_resistance",
            plate_id=plate_id,
            resistance=resistance
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to send set_resistance command"
            )

        return {"status": "success", "sent_to": node_id, "resistance": resistance}

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing field: {str(e)}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Voltage must be a float")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.post("/api/control/set-calibration")
async def set_calibration_control(message: Dict[str, Any]):
    """
    Send a 'set_calibration' control message to a specific node.
    Also updates the chx_cal_resistance in the plate table.
    Expected JSON:
    {
        "node_id": "node-abc123",
        "plate_id": "plate_1",
        "channel": "0"
        "targe_voltage": 1.0,
        "cal_voltage": 2.2
    }
    """
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    try:
        node_id = message["node_id"]
        plate_id = message["plate_id"]
        channel = message["channel"]
        target_voltage = message["target_voltage"]
        cal_voltage = message["cal_voltage"]


        # ✅ Send the control message to the node
        success = collector.send_control_message(
            node_id,
            action = "set_calibration",
            plate_id = plate_id,
            channel = channel,
            target_voltage = target_voltage,
            cal_voltage = cal_voltage
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to send set_calibration command"
            )

        return {"status": "success", "sent_to": node_id, "plate_id": plate_id, "channel": channel}

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing field: {str(e)}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Voltage must be a float")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# Run server when script is executed directly
if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
