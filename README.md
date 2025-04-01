# CELMES

[CEL]l [ME]asurement [S]ystem.

Measument system for REID cells.

Current setup consists of one server and multiple nodes. Communication between nodes and server is done via Zenoh - an open source, high-performance, and low-latency communication middleware.

# Installation

## Server

Server is responsible for storing the measurement data in a time series database

## Node

Node is responsible for reading the electronic signals from the pi plates and publishing them on the network.
