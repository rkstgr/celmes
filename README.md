# CELMES

[CEL]l [ME]asurement [S]ystem - A distributed system for REID cell measurements.

## Overview

CELMES consists of a central server and multiple measurement nodes communicating via Zenoh protocol. The system supports both hardware DAQ nodes and simulated nodes for development/testing.

## Components

### Server

-   FastAPI-based server with TimescaleDB backend
-   Stores measurement data (environmental, power, energy)
-   Manages node configurations and cell mappings
-   Provides REST API for data access and control

### Nodes

Two types of nodes are supported:

-   **DAQ Node**: Interfaces with real hardware (DAQC2 plates and BME280 sensor)
-   **Sim Node**: Simulates measurements for development/testing

Each node can manage multiple plates with 8 channels each, collecting:

-   Power/Energy measurements per channel
-   Environmental data (temperature, humidity, pressure)

## Installation

### Requirements

```
python >= 3.8
TimescaleDB
Zenoh
```

For hardware nodes:

-   DAQC2 plates
-   BME280 sensor
-   Raspberry Pi or compatible hardware

### Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Start the server:

```bash
python server/server.py
```

3. Run a node:

```bash
python node/run.py --type [daq|sim] --node-id [ID] --num-plates [N]
```
