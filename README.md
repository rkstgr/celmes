# CELMES

Cell Measurement System.

Distributed system of nodes with up two 8 * 7 cells, measuring voltage and energy.

## Setup

Repo is structured into `/node`, `/server` `/gui`

node: runs on each node, is responsible to talk with the pi plates and read the electronic signals
server: there should only one in the network, hosting the database to which the nodes upload the measurement data
gui: grafana frontend to visualize the time series data
