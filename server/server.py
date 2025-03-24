#!/usr/bin/env python3
"""
Server implementation for the measurement system using Zenoh and TimescaleDB
"""

import os
import json
import time
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import zenoh
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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

    def connect_db(self):
        """Connect to TimescaleDB"""
        try:
            logger.info(
                f"Connecting to database {self.db_params.get('dbname')} at {self.db_params.get('host')}:{self.db_params.get('port')}"
            )
            conn = psycopg2.connect(
                host=self.db_params.get("host", "localhost"),
                port=self.db_params.get("port", 5432),
                dbname=self.db_params.get("dbname", "measurement_db"),
                user=self.db_params.get("user", "postgres"),
                password=self.db_params.get("password", "password"),
            )
            logger.info("Successfully connected to database")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def setup_db(self):
        """Set up database tables and TimescaleDB hypertables"""
        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            # Create tables if they don't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT,
                properties JSONB,
                last_seen TIMESTAMP WITH TIME ZONE
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS environmental_data (
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                node_id TEXT NOT NULL,
                temperature FLOAT,
                humidity FLOAT,
                pressure FLOAT,
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS plate_data (
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                node_id TEXT NOT NULL,
                plate_id TEXT NOT NULL,
                reference_voltage FLOAT,
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_data (
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                node_id TEXT NOT NULL,
                plate_id TEXT NOT NULL,
                channel INTEGER NOT NULL,
                avg_power FLOAT,
                energy FLOAT,
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
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
            logger.info("Database setup complete")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error setting up database: {str(e)}")
        finally:
            cursor.close()
            conn.close()

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
        """Start collecting data from nodes"""
        if self.running:
            logger.info("Data collection already running, skipping startup")
            return

        self.running = True
        logger.info("Starting data collection")

        try:
            # Subscribe to all node data
            data_key_expr = "measurement/node/*/data"
            logger.info(f"Subscribing to node data with key: {data_key_expr}")
            self.subscriber = self.zenoh_session.declare_subscriber(
                data_key_expr, self._handle_data
            )
            logger.info(f"Successfully subscribed to node data")

            # Declare queryable for node discovery
            discovery_key_expr = "measurement/discovery"
            logger.info(
                f"Declaring queryable for discovery with key: {discovery_key_expr}"
            )
            self.discovery_queryable = self.zenoh_session.declare_queryable(
                discovery_key_expr, self._handle_discovery_query
            )
            logger.info(f"Successfully registered discovery queryable")

            # Verify the subscriber is working
            logger.info(
                f"Data collection service started and ready - waiting for node data"
            )

        except Exception as e:
            self.running = False
            logger.error(f"Error starting data collection: {str(e)}")
            raise

    def stop_collecting(self):
        """Stop collecting data"""
        if not self.running:
            return

        self.running = False
        if self.zenoh_session:
            self.zenoh_session.close()
            logger.info("Zenoh session closed")

    def _handle_data(self, sample):
        """Process incoming data from nodes"""
        try:
            # Add logging for the raw data received
            logger.info(f"Received data on key: {sample.key_expr}")

            # Parse data
            payload = json.loads(sample.payload.to_string())

            # Extract key fields
            node_id = payload.get("node_id")
            timestamp = payload.get("timestamp")
            data = payload.get("data", {})

            logger.info(f"Received data from node {node_id} at timestamp {timestamp}")

            if not node_id or not timestamp or not data:
                logger.warning("Missing required fields in data payload")
                return

            # Log data structure for debugging
            logger.info(f"Data contains: {', '.join(data.keys())}")

            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(timestamp)

            # Ensure node exists in database
            self._ensure_node_exists(node_id)

            # Process environmental data
            if "environment" in data:
                env_data = data["environment"]
                logger.info(
                    f"Processing environmental data: temp={env_data.get('temperature'):.1f}°C, humidity={env_data.get('humidity'):.1f}%, pressure={env_data.get('pressure'):.1f}hPa"
                )
                self._store_environmental_data(node_id, dt, env_data)

            # Process plate data
            if "plates" in data:
                logger.info(f"Processing data for {len(data['plates'])} plates")
                self._store_plate_data(node_id, dt, data["plates"])

            # Update node last seen
            self._update_node_last_seen(node_id, dt)

            logger.info(f"Successfully processed data from node {node_id}")

        except json.JSONDecodeError:
            logger.error(
                f"Failed to parse data payload: {sample.payload.decode('utf-8', errors='replace')}"
            )
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")

    def _handle_discovery_query(self, query):
        """Handle discovery queries"""
        try:
            # Return list of known nodes
            nodes_list = list(self.nodes.values())
            query.reply(
                json.dumps({"nodes": nodes_list, "timestamp": int(time.time())})
            )
        except Exception as e:
            logger.error(f"Error handling discovery query: {str(e)}")

    def _ensure_node_exists(self, node_id):
        """Make sure the node exists in the database"""
        if node_id in self.nodes:
            return

        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            # Check if node exists
            cursor.execute("SELECT 1 FROM nodes WHERE node_id = %s", (node_id,))
            if not cursor.fetchone():
                # Insert new node
                cursor.execute(
                    "INSERT INTO nodes (node_id, last_seen) VALUES (%s, NOW())",
                    (node_id,),
                )
                conn.commit()
                logger.info(f"Added new node: {node_id}")

            # Add to in-memory cache
            self.nodes[node_id] = {
                "node_id": node_id,
                "last_seen": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error ensuring node exists: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def _update_node_last_seen(self, node_id, timestamp):
        """Update node's last seen timestamp"""
        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE nodes SET last_seen = %s WHERE node_id = %s",
                (timestamp, node_id),
            )

            conn.commit()

            # Update in-memory cache
            if node_id in self.nodes:
                self.nodes[node_id]["last_seen"] = timestamp.isoformat()

        except Exception as e:
            logger.error(f"Error updating node last seen: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def _store_environmental_data(self, node_id, timestamp, env_data):
        """Store environmental data in TimescaleDB"""
        try:
            logger.info(
                f"Connecting to DB to store environmental data for node {node_id}"
            )
            conn = self.connect_db()
            cursor = conn.cursor()

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

            conn.commit()
            logger.info(f"Successfully stored environmental data for node {node_id}")

        except Exception as e:
            logger.error(f"Error storing environmental data: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def _store_plate_data(self, node_id, timestamp, plates_data):
        """Store plate and channel data in TimescaleDB"""
        try:
            logger.info(f"Connecting to DB to store plate data for node {node_id}")
            conn = self.connect_db()
            cursor = conn.cursor()

            total_channels = 0
            for plate in plates_data:
                plate_id = plate.get("plate_id")
                ref_voltage = plate.get("reference_voltage")

                if not plate_id:
                    logger.warning("Skipping plate with missing plate_id")
                    continue

                logger.info(
                    f"Storing data for plate {plate_id} with ref voltage {ref_voltage}V"
                )

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
                    avg_power = channel_data.get("avg_power")
                    energy = channel_data.get("energy")

                    cursor.execute(
                        """
                        INSERT INTO channel_data
                        (time, node_id, plate_id, channel, avg_power, energy)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (timestamp, node_id, plate_id, channel, avg_power, energy),
                    )
                    total_channels += 1

            conn.commit()
            logger.info(
                f"Successfully stored data for {len(plates_data)} plates and {total_channels} channels from node {node_id}"
            )

        except Exception as e:
            logger.error(f"Error storing plate data: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def get_nodes(self):
        """Get list of all nodes from database"""
        try:
            conn = self.connect_db()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cursor.execute(
                """
                SELECT node_id, name, location, properties, last_seen
                FROM nodes
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
            cursor.close()
            conn.close()

    def update_node_config(self, node_config: NodeConfig):
        """Update node configuration"""
        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE nodes
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
                    INSERT INTO nodes (node_id, name, location, properties, last_seen)
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
            cursor.close()
            conn.close()

    def send_control_message(self, node_id: str, action: str, **params):
        """Send control message to node"""
        if not self.zenoh_session:
            logger.error("Zenoh session not initialized")
            return False

        try:
            # Create message
            message = {"action": action, "timestamp": int(time.time()), **params}

            # Send via Zenoh
            publisher = self.zenoh_session.declare_publisher(
                f"measurement/node/{node_id}/control"
            )
            publisher.put(json.dumps(message))

            logger.info(f"Sent {action} command to node {node_id}")
            return True

        except Exception as e:
            logger.error(f"Error sending control message: {str(e)}")
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

    yield

    # Shutdown
    if collector:
        collector.stop_collecting()


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
    finally:
        cursor.close()
        conn.close()


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
        SELECT time, plate_id, channel, avg_power, energy
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
    finally:
        cursor.close()
        conn.close()


# Run server when script is executed directly
if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
