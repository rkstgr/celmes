#!/usr/bin/env python3
"""
daq_node.py
Implementation of a measurement node with DAQ hardware integration
"""

import logging
import time
import numpy as np
from generic_node import GenericNode

logger = logging.getLogger("DaqNode")

# Try to import hardware modules
try:
    import smbus2
    import bme280
    import piplates.DAQC2plate as DAQC2

    HARDWARE_AVAILABLE = True
except ImportError:
    logger.error(
        "Failed to import required hardware modules. Cannot initialize hardware."
    )
    HARDWARE_AVAILABLE = False


class DaqNode(GenericNode):
    """Implementation of a measurement node that collects data from real hardware"""

    def __init__(
        self, node_id="node-001", num_plates=4, buffer_db_path="measurement_buffer.db"
    ):
        super().__init__(node_id, num_plates, buffer_db_path)

        if not HARDWARE_AVAILABLE:
            raise RuntimeError(
                "Hardware modules are not available, cannot initialize DaqNode"
            )

        # Initialize DAQC2 plates
        self.daqc2_present = DAQC2.daqc2sPresent
        self.plates = []

        # Only initialize plates that are actually present
        for plate_idx in range(8):  # DAQC2 supports addresses 0-7
            if self.daqc2_present[plate_idx] == 1:
                channels = []
                for channel_idx in range(8):  # DAQC2 has 8 channels (0-7)
                    channels.append(
                        {
                            "power": np.zeros(self.array_size),  # Will be calculated from voltage
                            "energy": 0.0,  # Wh tied to cell_id
                            "cell_id": None,
                            "voltage": np.zeros(self.array_size),  # Raw voltage reading
                            "timestamp": np.array([""] * self.array_size, dtype='<U40')  # 40 chars is safe for this format
,
                        }
                    )

                self.plates.append(
                    {
                        "plate_id": f"plate-{plate_idx}",
                        "address": plate_idx,
                        "target_voltage": 1.200,
                        "channels": channels,
                    }
                )

        logger.info(f"Initialized {len(self.plates)} DAQC2 plates")

        # Initialize BME280 sensor
        self.bme280_address = 0x76  # Default BME280 address
        try:
            self.i2c_bus = smbus2.SMBus(1)
            self.bme280_calibration = bme280.load_calibration_params(
                self.i2c_bus, self.bme280_address
            )
            logger.info("BME280 sensor initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BME280 sensor: {str(e)}")
            self.i2c_bus = None
            self.bme280_calibration = None

    def _update_environmental_data(self):
        """Get real environmental data from BME280 sensor"""
        if self.i2c_bus is None or self.bme280_calibration is None:
            # Fall back to keeping current values if sensor initialization failed
            logger.warning("BME280 sensor not initialized, keeping current values")
            return

        try:
            # Read actual sensor data
            sensor_data = bme280.sample(
                self.i2c_bus, self.bme280_address, self.bme280_calibration
            )

            # Update environmental data with real readings
            self.env_data["temperature"][self.i] = sensor_data.temperature
            self.env_data["pressure"][self.i] = sensor_data.pressure
            self.env_data["humidity"][self.i] = sensor_data.humidity

            logger.debug(
                f"BME280 readings - Temp: {self.env_data['temperature'][i]:.1f}°C, "
                f"Pressure: {self.env_data['pressure'][i]:.1f}hPa, "
                f"Humidity: {self.env_data['humidity'][i]:.1f}%"
            )

        except Exception as e:
            logger.error(f"Error reading BME280 sensor: {str(e)}")
            # Keep current values if read fails

    def _update_plate_readings(self):
        """Update power and energy readings from actual DAQC2 plates"""
        node_reading_start = time.time()
        try:
            plate_reading_times = []
            for plate in self.plates:
                plate_reading_start = time.time()
                address = plate["address"]
                target_voltage = plate["target_voltage"]
                
                for idx, channel in enumerate(plate["channels"]):
                    try:
                        # Read voltage from DAQC2
                        voltage = DAQC2.getADC(address, idx)
                        channel["voltage"][self.i] = voltage
                        channel["timestamp"][self.i] = self._format_timestamp()
                        # channel 0 is always the reference voltage
                        if idx == 0:
                            ref_voltage = voltage
                        else:
                            # Calculate power in mW
                            power = ((voltage - ref_voltage) / 22) * voltage * 1000
                            channel["power"][self.i] = power

                            # Compute trapezoidal energy if we have a previous entry
                            prev_i = (self.i - 1) % self.array_size  #note: -1 % 10 = 9
                            prev_power = channel["power"][prev_i]
                            prev_timestamp_str = channel["timestamp"][prev_i]
                            curr_timestamp_str = channel["timestamp"][self.i]

                            # Format: "2025-04-10 13:22:45:123456 +0200"
                            time_format = "%Y-%m-%d %H:%M:%S:%f %z"

                            try:
                                t1 = datetime.strptime(prev_timestamp_str, time_format)
                                t2 = datetime.strptime(curr_timestamp_str, time_format)
                                delta_seconds = (t2 - t1).total_seconds()

                                # Trapezoidal rule energy integration
                                avg_power_mW = (prev_power + power) / 2
                                delta_energy_Wh = (avg_power_mW / 1000) * (delta_seconds / 3600)
                                channel["energy"] += delta_energy_Wh

                            except Exception as e:
                                logger.warning(f"Could not parse timestamps for energy calc: {e}")

                # Keep previous values on error
                plate_reading_times.append(time.time() - plate_reading_start)

        except Exception as e:
            logger.error(f"Error updating plate readings: {str(e)}")
            # If hardware read fails, don't update values

        node_reading_time = time.time() - node_reading_start
        logger.debug(f"Node reading time: {node_reading_time * 1000:.1f}ms")
        if plate_reading_times:
            logger.debug(
                f"Plate reading times: [{', '.join([f'{t * 1000:.1f}ms' for t in plate_reading_times])}]"
            )
