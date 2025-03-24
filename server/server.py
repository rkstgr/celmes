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
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "qpm_celmes")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASSWORD", "password")


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

            logger.info("Data collection started - Zenoh ready")

        except Exception as e:
            self.running = False
            logger.error(f"Failed to start collection: {str(e)}")
            raise

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

    def stop_collecting(self):
        """Stop collecting data"""
        if not self.running:
            return

        self.running = False

        # Stop heartbeat thread
        if hasattr(self, "heartbeat_thread"):
            self.heartbeat_thread.join(timeout=2)

        if self.zenoh_session:
            self.zenoh_session.close()
            logger.info("Zenoh session closed")

        self.close_db()  # Close database connection when stopping

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

            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %z")
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

                if not plate_id:
                    logger.warning("Skipping plate with missing plate_id")
                    continue

                # Store plate data
                cursor.execute(
                    """
                    INSERT INTO plate_data
                    (time, node_id, plate_id, reference_voltage)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (timestamp, node_id, plate_id, ref_voltage),
                )

                # Store channel data
                channels = plate.get("channels", [])
                for channel_data in channels:
                    channel = channel_data.get("channel")
                    power_mW = channel_data.get("power_mW")
                    energy_Wh = channel_data.get("energy_Wh")

                    # Check if this path already has a mapping
                    cursor.execute(
                        """
                        SELECT 1 FROM path_cell_mapping
                        WHERE node_id = %s AND plate_id = %s AND channel = %s
                        AND end_time IS NULL
                        """,
                        (node_id, plate_id, channel),
                    )

                    # If no mapping exists, create default mapping to 'unconfigured'
                    if not cursor.fetchone():
                        logger.debug(
                            f"New mapping: {node_id}/{plate_id}/channel-{channel} -> unconfigured"
                        )
                        cursor.execute(
                            """
                            INSERT INTO path_cell_mapping
                            (node_id, plate_id, channel, cell_id, start_time)
                            VALUES (%s, %s, %s, 'unconfigured', %s)
                            """,
                            (node_id, plate_id, channel, timestamp),
                        )

                    # Get current cell_id for this path using same connection
                    cell_id = self._get_cell_for_path(
                        node_id, plate_id, channel, timestamp, cursor
                    )

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
                            cell_id,
                            power_mW,
                            energy_Wh,
                        ),
                    )

                cursor.execute(
                    """
                    UPDATE path_cell_mapping
                    SET end_time = %s
                    WHERE node_id = %s AND plate_id = %s AND channel = %s
                    AND end_time IS NULL
                    """,
                    (datetime.now(timezone.utc), node_id, plate_id, channel),
                )

                self.db_conn.commit()

        except Exception as e:
            logger.error(f"Plate data storage failed for {node_id}: {str(e)}")
            if isinstance(e, psycopg2.Error):
                self.close_db()
            raise
        finally:
            cursor.close()

    def _get_cell_for_path(self, node_id, plate_id, channel, timestamp, cursor):
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
            return result[0] if result else "unconfigured"
        except Exception as e:
            logger.error(f"Error getting cell for path: {str(e)}")
            return "unconfigured"
        # Don't close cursor here since it's passed in

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
                f"Node {node_id}: reassigned {plate_id}/ch{channel} -> {new_cell_id}"
            )
        finally:
            cursor.close()


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


@app.post("/api/control/reference-voltage")
async def set_reference_voltage(command: SetReferenceVoltage):
    """Set reference voltage for a plate"""
    global collector

    if not collector:
        raise HTTPException(status_code=503, detail="Service not available")

    success = collector.send_control_message(
        command.node_id,
        "set_reference_voltage",
        plate_id=command.plate_id,
        value=command.value,
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to send control message")

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


# Run server when script is executed directly
if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
