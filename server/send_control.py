#!/usr/bin/env python3

import argparse
import json
import requests

def send_assign_cell(endpoint, node_id, plate_id, channel, cell_id, energy_wh=None):
    url = f"{endpoint}/api/control/assign-cell"

    payload = {
        "node_id": node_id,
        "plate_id": plate_id,
        "channel": channel,
        "cell_id": cell_id,
    }

    if energy_wh is not None:
        payload["energy_wh"] = energy_wh

    print(f"📡 Sending assign_cell: {payload}")
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        print("✅ Assign_cell successful:", response.json())
    else:
        print("❌ Assign_cell failed:", response.status_code, response.text)


def send_set_reference_voltage(endpoint, node_id, plate_id, target_voltage):
    url = f"{endpoint}/api/control/set-reference-voltage"
    payload = {
        "node_id": node_id,
        "plate_id": plate_id,
        "target_voltage": target_voltage
    }

    print(f"📡 Sending 'set_reference_voltage' control message to {url}:\n{json.dumps(payload, indent=2)}")
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        print("✅ Message sent successfully.")
    else:
        print(f"❌ Error {response.status_code}: {response.text}")


def main():
    parser = argparse.ArgumentParser(description="Send control messages to the measurement node via FastAPI server.")
    parser.add_argument("--endpoint", default="http://localhost:8000", help="Server REST API endpoint (default: http://localhost:8000)")
    parser.add_argument("--action", required=True, choices=["assign_cell", "set_reference_voltage"], help="Control action")
    parser.add_argument("--node-id", required=True, help="Node ID (e.g., node-abc123)")
    parser.add_argument("--plate-id", help="Plate ID (e.g., plate_1)")
    parser.add_argument("--channel", type=int, help="Channel number (int, required for assign_cell)")
    parser.add_argument("--cell-id", type=str, help="Cell ID (int, required for assign_cell)")
    parser.add_argument("--target-voltage", type=float, help="Voltage value (float, required for set_reference_voltage)")
    parser.add_argument("--energy-wh", type=float, help="Energy value (float, required for assign_cell)")

    args = parser.parse_args()

    if args.action == "assign_cell":
        if args.plate_id is None or args.channel is None or args.cell_id is None:
            parser.error("--plate-id, --channel, and --cell-id are required for 'assign_cell'")
        send_assign_cell(args.endpoint, args.node_id, args.plate_id, args.channel, args.cell_id, args.energy_wh)

    elif args.action == "set_reference_voltage":
        if args.plate_id is None or args.target_voltage is None:
            parser.error("--plate-id and --target-voltage are required for 'set_reference_voltage'")
        send_set_reference_voltage(args.endpoint, args.node_id, args.plate_id, args.target_voltage)


if __name__ == "__main__":
    main()
