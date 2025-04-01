#!/usr/bin/env python3
"""
sim_node.py
Implementation of a simulated measurement node
"""

import logging
import numpy as np
import time
from generic_node import GenericNode

logger = logging.getLogger("SimNode")


class SimNode(GenericNode):
    """Implementation of a measurement node that simulates data"""

    def __init__(self, node_id="sim-001", num_plates=4, buffer_db_path="sim_buffer.db"):
        super().__init__(node_id, num_plates, buffer_db_path)

        # Initialize simulated plates
        self.plates = []
        for plate_idx in range(num_plates):
            channels = []
            for channel_idx in range(
                8
            ):  # Using 8 channels for consistency with real hardware
                channels.append(
                    {
                        "power": 1.05,  # mW (starting value)
                        "energy": 0.0,  # Wh
                        "cell_id": None,
                        "voltage": 0.48,  # Simulated voltage (√(P*R/1000))
                        "timestamp": self._format_timestamp(),
                    }
                )

            self.plates.append(
                {
                    "plate_id": f"plate-{plate_idx}",
                    "address": plate_idx,  # Including address for consistency
                    "reference_voltage": 3.3,
                    "channels": channels,
                }
            )

        logger.info(f"Initialized {len(self.plates)} simulated plates")

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
        """Update power and energy readings with simulated data"""
        for plate in self.plates:
            ref_voltage = plate["reference_voltage"]
            voltage_factor = ref_voltage / 3.3  # Scale with reference voltage

            for channel in plate["channels"]:
                # Generate power with Gaussian noise around 1.05mW
                base_power = 1.05 * voltage_factor
                noise = np.random.normal(0, 0.01)
                power = max(0, base_power + noise)
                channel["power"] = power

                # Simulate voltage (V = sqrt(P*R/1000)), assuming a 22Ω resistor
                # Convert from mW to W (*0.001) for calculation
                channel["voltage"] = (power * 0.001 * 22) ** 0.5
                channel["timestamp"] = self._format_timestamp()

                # Update energy (Wh)
                channel["energy"] += (power / 1000) / 3600
