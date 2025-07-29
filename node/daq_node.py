#!/usr/bin/env python3
"""
daq_node.py
Implementation of a measurement node with DAQ hardware integration
"""

import logging
import time
import numpy as np
from datetime import datetime
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
        initial_timestamp = self._format_timestamp()
        
        # Only initialize plates that are actually present
        for plate_idx in range(8):  # DAQC2 supports addresses 0-7
            if self.daqc2_present[plate_idx] == 1:
                channels = []
                DAQC2.setLED(plate_idx, 'blue')
                DAQC2.setDOUTall(plate_idx, 0x00)
                time.sleep(0.2)
                for channel_idx in range(8):  # DAQC2 has 8 channels (0-7)
                    channels.append(
                        {
                            "power": np.zeros(self.array_size),  # Will be calculated from voltage
                            "energy": 0.0,  # Wh tied to cell_id
                            "cell_id": None,
                            "cal_resistance": 0.0,
                            "voltage": np.zeros(self.array_size),  # Raw voltage reading
                            "timestamp": np.array([initial_timestamp] * self.array_size, dtype='<U40'),  # 40 chars is safe for this format
                        }
                    )

                self.plates.append(
                    {
                        "plate_id": f"plate-{plate_idx}",
                        "address": plate_idx,
                        "target_voltage": 1.200,
                        "resistance": 22.0,
                        "bias_voltage": 0.5,  # Initial DAC bias voltage for BJT control
                        "channels": channels,
                    }
                )
        for plate_idx in range(8):  # DAQC2 supports addresses 0-7
            DAQC2.setLED(plate_idx, 'off')
            
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
#             logger.warning("BME280 sensor not initialized, keeping current values")
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
                f"BME280 readings - Temp: {self.env_data['temperature'][self.i]:.1f}°C, "
                f"Pressure: {self.env_data['pressure'][self.i]:.1f}hPa, "
                f"Humidity: {self.env_data['humidity'][self.i]:.1f}%"
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
                resistance = plate["resistance"]
                
                for idx in reversed(range(len(plate["channels"]))):
                    try:
                        channel = plate["channels"][idx]
                        # Read voltage from DAQC2
                        voltage = DAQC2.getADC(address, idx)
                        channel["voltage"][self.i] = voltage
                        channel["timestamp"][self.i] = self._format_timestamp()
                        # channel 7 is always the reference voltage
                        if idx == 7:
                            ref_voltage = voltage
                        else:
                            # Calculate power in mW
                            power = ((voltage - ref_voltage) / (resistance + channel["cal_resistance"])) * voltage * 1000
                            channel["power"][self.i] = power

                            # Compute trapezoidal energy if we have a previous entry
                            prev_i = (self.i - 1) % self.array_size  #note: -1 % 10 = 9
                            prev_power = channel["power"][prev_i]
                            prev_timestamp_str = channel["timestamp"][prev_i]
                            curr_timestamp_str = channel["timestamp"][self.i]

                            # Format: "2025-04-10 13:22:45:123456 +0200"
                            time_format = "%Y-%m-%d %H:%M:%S:%f %z"

#                             if address == 0:
#                                 print(f"ch-{idx}, voltages: {channel['voltage']}", flush=True)

                            try:
                                t1 = datetime.strptime(prev_timestamp_str, time_format)
                                t2 = datetime.strptime(curr_timestamp_str, time_format)
                                delta_seconds = (t2 - t1).total_seconds()

                                # Trapezoidal rule energy integration
                                avg_power_mW = (prev_power + power) / 2
                                delta_energy_Wh = (avg_power_mW / 1000) * (delta_seconds / 3600)
                                channel["energy"] += delta_energy_Wh
#                                 if idx == 1:
#                                     print(f"delta_seconds: {delta_seconds}", flush=True)
#                                     print(f"avg_power_mW: {avg_power_mW}", flush=True)
#                                     print(f"delta_energy_Wh: {delta_energy_Wh}", flush=True)
#                                     print(f"channel['energy']: {channel['energy']}", flush=True)

                            except Exception as e:
                                logger.warning(f"Could not parse timestamps for energy calc: {e}")

                    except Exception as e:
                        logger.error(
                            f"Error reading channel {idx} on plate {address}: {str(e)}"
                        )
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
            
    def _update_reference_voltage(self):
        """Update reference voltage by adjusting bias DAC output"""
        
        Kp = 0.3               # Proportional gain — tune this down to reduce reaction strength
        max_step = 0.1        # Maximum allowed DAC change per update (V)
        min_dac = 0.0          # Min DAC output (e.g., 0V)
        max_dac = 4.095          # Max DAC output (e.g., 3.3V)

        for plate in self.plates:
            address = plate["address"]
            target_voltage = plate["target_voltage"]

            # Read the current reference voltage from buffer (averaged over last 10 seconds)
            reference_voltage = np.mean(plate["channels"][7]["voltage"])

            # Compute the error
            error = target_voltage - reference_voltage

            # Calculate bias correction step
            step = Kp * error
            step = np.clip(step, -max_step, max_step)

            # Update the internal DAC bias (clamped to valid range)
            previous_bias = plate.get("bias_voltage", 0.5)  # default starting point
            new_bias = np.clip(previous_bias + step, min_dac, max_dac)

            # Set DAC
            DAQC2.setDAC(address, 0, new_bias)

            # Store the updated value for next iteration
            plate["bias_voltage"] = new_bias
#             if address == 0:
#                 logger.info(f"[Plate {address}] Ref={reference_voltage:.3f}V, Target={target_voltage:.3f}V, Bias set to {new_bias:.3f}V")

    def _update_channel_calibration(self, plate_id, channel, target_voltage, cal_voltage):
        self.i = 0
        count = 0
        cal_varience = 1.0
        reference_voltage = 0.0
        stability_check = False
        cal_resistance_array = np.zeros(self.array_size)
        info_msg_flag = True
        
        for plate in self.plates:
            if plate["plate_id"] == plate_id:
                logger.info(f"Setting calibration resistance for {plate_id} and ch-{channel} with target_voltage: {target_voltage} and cal_voltage: {cal_voltage}")
                plate["target_voltage"] = target_voltage
                while(True):
                    DAQC2.toggleDOUTbit(plate["address"], channel)
                    self._update_plate_readings()
                    
                    if count > 10:
                        if count == 11:
                            logger.info(f"Voltage Arrays Filled... Stabilizing Reference Voltage to Target: {plate['target_voltage']}V")
                            
                        self._update_reference_voltage()
                        reference_voltage = np.mean(plate["channels"][7]["voltage"])
                        
                        if reference_voltage > (plate["target_voltage"] - 0.002) and reference_voltage < (plate["target_voltage"] + 0.002):
                            if info_msg_flag:
                                logger.info(f"Reference Voltage Stabilized, Calculating Calibration Resistance...")
                                info_msg_flag = False
                            DAQC2.toggleDOUTbit(plate["address"], channel)
                            channel_voltage = np.mean(plate["channels"][channel]["voltage"])
                            cal_resistance_array[self.i] = (cal_voltage - channel_voltage) / ((channel_voltage - reference_voltage)/plate["resistance"])
                            cal_resistance = np.mean(cal_resistance_array)
                            cal_varience = np.var(cal_resistance_array)
                            if cal_varience > 0.1:
                                DAQC2.setLED(plate["address"], 'red')
                            elif cal_varience > 0.05:
                                DAQC2.setLED(plate["address"], 'yellow')
                            elif cal_varience > 0.02:
                                DAQC2.setLED(plate["address"], 'green')

                            logger.info(f"avg_cal: {cal_resistance}, varience: {cal_varience}")
                            
                    self.i += 1
                    count += 1    
                    # Reset index to 0 after full cycle
                    if self.i == self.array_size:
                        self.i = 0
                        if reference_voltage > (plate["target_voltage"] - 0.002) and reference_voltage < (plate["target_voltage"] + 0.002):
                            stability_check = True
                    if stability_check and cal_varience < 0.005:
                        logger.info(f"SUCCESS!!! Stable calibration found for plate_id: {plate_id} channel: {channel}")
                        DAQC2.clrDOUTbit(plate["address"], channel)
                        DAQC2.setLED(plate["address"], 'off')
                        return cal_resistance
                    # 30 second timeout    
                    if count > 100:
                        DAQC2.clrDOUTbit(plate["address"], channel)
                        DAQC2.setLED(plate["address"], 'off')
                        logger.info(f"Stable calibration not found for plate_id: {plate_id} channel: {channel}")
                        return None
                    
                    time.sleep(0.1)
                    
            