#!/usr/bin/env python3
"""
Simplified measurement node simulation using Zenoh
"""

import time
import json
import random
import uuid
import logging
import numpy as np
import zenoh

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SimNode")

NODE_ID = "node-000"


class SimulatedNode:
    """Simulates a Raspberry Pi measurement node with multiple Pi-plates"""

    def __init__(self, node_id=None, num_plates=4):
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.num_plates = num_plates

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
                # Base power is now around 1.05mW with low variance
                channels.append(
                    {
                        "power": 1.05,  # mW
                        "energy": 0.0,  # Wh
                        "cell_id": None,  # Initialize with no cell assigned
                    }
                )

            self.plates.append(
                {
                    "plate_id": f"plate-{plate_idx}",
                    "reference_voltage": 3.3,  # Default reference voltage
                    "channels": channels,
                }
            )

        # Initialize Zenoh session
        self.zenoh_session = None
        self.start_time = time.time()

    def connect_zenoh(self, config=None):
        """Connect to Zenoh network"""
        logger.info("Connecting to Zenoh network")
        self.zenoh_session = zenoh.open(zenoh.Config() if config is None else config)

        # Initialize publisher for data
        self.data_publisher = self.zenoh_session.declare_publisher(
            f"measurement/node/{self.node_id}/data"
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
        """Simulate small changes in environmental data"""
        self.env_data["temperature"] += np.random.normal(0, 0.05)  # Smaller changes
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
                # Scale by reference voltage
                base_power = 1.05 * voltage_factor
                noise = np.random.normal(0, 0.01)  # Very low variance
                power = base_power + noise
                channel["power"] = max(0, power)  # Prevent negative power

                # Update energy (Wh) - convert mW to W and divide by 3600 for Wh per second
                channel["energy"] += channel["power"] / 1000 / 3600

    def _publish_data(self):
        """Publish environmental and measurement data to Zenoh"""
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
                        "cell_id": channel[
                            "cell_id"
                        ],  # Include cell_id in published data
                    }
                )

            payload["data"]["plates"].append(plate_data)

        # Send data via Zenoh
        if self.zenoh_session:
            self.data_publisher.put(json.dumps(payload))
        else:
            logger.error("Zenoh session not connected")
            exit(1)

    def _handle_control_message(self, sample):
        """Handle incoming control messages"""
        try:
            message = json.loads(sample.payload.to_string())
            logger.info(f"Received control message: {message}")

            # Parse timestamp if present
            if "timestamp" in message:
                try:
                    message_time = self._parse_timestamp(message["timestamp"])
                    logger.debug(f"Message timestamp: {message_time}")
                except ValueError:
                    logger.warning(
                        f"Invalid timestamp format in message: {message['timestamp']}"
                    )

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

    def _parse_timestamp(self, timestamp_str):
        """Parse a timestamp string in the format YYYY-MM-DD HH:MM:SS +ZZZZ"""
        from datetime import datetime

        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S %z")

    def run(self):
        """Main simulation loop - publish data every second"""
        logger.info(
            f"Starting simulated node {self.node_id} with {self.num_plates} plates"
        )

        try:
            while True:
                # Update data
                self._update_environmental_data()
                self._update_plate_readings()

                # Publish data every second
                self._publish_data()
                logger.debug(f"Published data for node {self.node_id}")

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
    node = SimulatedNode(node_id=NODE_ID, num_plates=2)

    # Connect to Zenoh
    node.connect_zenoh()

    # Run the simulation
    node.run()


if __name__ == "__main__":
    main()
