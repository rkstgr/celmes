#!/usr/bin/env python3
"""
Simulated measurement node using Zenoh for communication
"""

import time
import json
import random
import uuid
import logging
from collections import deque
from typing import Dict, List, Any
import zenoh
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SimNode")


class SimulatedNode:
    """
    Simulates a Raspberry Pi measurement node with multiple Pi-plates
    """

    def __init__(self, node_id=None, num_plates=4):
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.num_plates = num_plates

        # Initialize simulated sensors
        self.env_data = {
            "temperature": 21.0,  # Celsius
            "humidity": 45.0,  # %
            "pressure": 1013.0,  # hPa
        }

        # Initialize simulated Pi-plates with channels
        self.plates = []
        for plate_idx in range(num_plates):
            channels = []
            for channel_idx in range(7):
                # Each channel has a moving average of power readings
                channels.append(
                    {
                        "power_readings": deque(maxlen=10),
                        "avg_power": 0.0,  # mW
                        "energy": 0.0,  # Wh
                        "last_power": 0.0,  # Last raw reading
                    }
                )

            self.plates.append(
                {
                    "plate_id": f"plate-{plate_idx}",
                    "reference_voltage": 3.3,  # Default reference voltage
                    "channels": channels,
                }
            )

        # Initialize counters for 10-second events
        self.counter = 0

        # Initialize Zenoh session
        self.zenoh_session = None

    def connect_zenoh(self, config=None):
        """Connect to Zenoh network"""
        logger.info("Connecting to Zenoh network")
        self.zenoh_session = zenoh.open(zenoh.Config() if config is None else config)

        # Initialize publisher for data
        self.data_publisher = self.zenoh_session.declare_publisher(
            f"measurement/node/{self.node_id}/data"
        )

        # Initialize queryable to respond to control messages
        self.control_queryable = self.zenoh_session.declare_queryable(
            f"measurement/node/{self.node_id}/control", self._handle_control_query
        )

        # Subscribe to control messages
        self.control_subscriber = self.zenoh_session.declare_subscriber(
            f"measurement/node/{self.node_id}/control", self._handle_control_message
        )

        logger.info(f"Node {self.node_id} connected to Zenoh")

    def close(self):
        """Close Zenoh session"""
        if self.zenoh_session:
            self.zenoh_session.close()
            logger.info("Zenoh session closed")

    def _update_environmental_data(self):
        """Simulate changes in environmental data"""
        self.env_data["temperature"] += random.uniform(-0.1, 0.1)
        self.env_data["humidity"] += random.uniform(-0.5, 0.5)
        self.env_data["pressure"] += random.uniform(-0.2, 0.2)

        # Keep values in reasonable ranges
        self.env_data["temperature"] = max(15, min(30, self.env_data["temperature"]))
        self.env_data["humidity"] = max(30, min(70, self.env_data["humidity"]))
        self.env_data["pressure"] = max(990, min(1030, self.env_data["pressure"]))

    def _update_plate_readings(self):
        """Simulate power and energy readings for each plate and channel"""
        for plate in self.plates:
            ref_voltage = plate["reference_voltage"]

            for idx, channel in enumerate(plate["channels"]):
                # Simulate power based on channel number (some channels use more power)
                base_power = 100 * (idx + 1)  # Base power in mW

                # Add some randomness and time-based patterns
                time_factor = 1.0 + 0.1 * np.sin(time.time() / 3600)  # Daily pattern
                noise = random.uniform(0.8, 1.2)

                # Reference voltage affects the reading
                voltage_factor = ref_voltage / 3.3

                # Calculate new power reading
                power = base_power * time_factor * noise * voltage_factor
                channel["last_power"] = power

                # Update moving average
                channel["power_readings"].append(power)
                channel["avg_power"] = sum(channel["power_readings"]) / len(
                    channel["power_readings"]
                )

                # Update energy (Wh) - convert mW to W and divide by 3600 for Wh per second
                channel["energy"] += power / 1000 / 3600

    def _save_data(self, env_data=True, measurement_data=True):
        """Save environmental and measurement data to Zenoh"""
        timestamp = int(time.time())

        payload = {"node_id": self.node_id, "timestamp": timestamp, "data": {}}

        # Add environmental data if needed
        if env_data:
            payload["data"]["environment"] = self.env_data.copy()

        # Add plate data if needed
        if measurement_data:
            plates_data = []
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
                            "avg_power": round(channel["avg_power"], 2),
                            "energy": round(channel["energy"], 4),
                        }
                    )

                plates_data.append(plate_data)

            payload["data"]["plates"] = plates_data

        # Send data via Zenoh
        if self.zenoh_session:
            self.data_publisher.put(json.dumps(payload))

    def _handle_control_message(self, sample):
        """Handle incoming control messages"""
        try:
            message = json.loads(sample.payload.decode("utf-8"))
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
                                    "timestamp": int(time.time()),
                                }
                            )
                        )

        except json.JSONDecodeError:
            logger.error("Failed to parse control message")
        except Exception as e:
            logger.error(f"Error handling control message: {str(e)}")

    def _handle_control_query(self, query):
        """Handle queries for node status and control"""
        try:
            selector = query.selector

            # Check what information is being requested
            if "/status" in selector:
                # Respond with node status
                status_data = {
                    "node_id": self.node_id,
                    "status": "online",
                    "plates": len(self.plates),
                    "uptime": int(time.time()),  # Just using current time for sim
                    "timestamp": int(time.time()),
                }
                query.reply(json.dumps(status_data))

            elif "/config" in selector:
                # Respond with configuration
                config_data = {
                    "node_id": self.node_id,
                    "plates": [
                        {
                            "plate_id": plate["plate_id"],
                            "reference_voltage": plate["reference_voltage"],
                        }
                        for plate in self.plates
                    ],
                    "timestamp": int(time.time()),
                }
                query.reply(json.dumps(config_data))

        except Exception as e:
            logger.error(f"Error handling query: {str(e)}")

    def _set_reference_voltage(self, plate_id, voltage):
        """Set reference voltage for a specific plate"""
        # Validate voltage is within acceptable range
        if not (1.0 <= voltage <= 5.0):
            logger.warning(f"Voltage {voltage}V out of acceptable range")
            return False

        # Find the plate
        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                logger.info(f"Setting reference voltage for {plate_id} to {voltage}V")
                plate["reference_voltage"] = voltage
                return True

        logger.warning(f"Plate {plate_id} not found")
        return False

    def run(self):
        """Main simulation loop"""
        logger.info(
            f"Starting simulated node {self.node_id} with {self.num_plates} plates"
        )

        try:
            while True:
                # Update data every second
                self._update_environmental_data()
                self._update_plate_readings()

                # Increment counter
                self.counter += 1

                # Every 10 seconds, save environmental and measurement data
                if self.counter % 10 == 0:
                    self._save_data(env_data=True, measurement_data=True)
                    logger.info(f"Saved full data for node {self.node_id}")

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
    # Create a simulated node
    node = SimulatedNode(num_plates=4)

    # Connect to Zenoh
    # For a real implementation you might add config like:
    # config = {"peers": ["tcp/192.168.1.100:7447"]}
    node.connect_zenoh()

    # Run the simulation
    node.run()


if __name__ == "__main__":
    main()
