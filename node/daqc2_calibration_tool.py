import argparse
import json
import os
from piplates.DAQC2plate import CalGetByte, CalPutByte

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CAL_FILE = os.path.join(SCRIPT_DIR, "plate_calibrations.json")


def get_plate_calibration(addr):
    return [CalGetByte(addr, ptr) for ptr in range(256)]


def write_plate_calibration(addr, data):
    for ptr, byte in enumerate(data):
        CalPutByte(addr, byte)


def read_all_plates():
    return {f"plate-{i}": get_plate_calibration(i) for i in range(8)}


def save_calibration():
    with open(CAL_FILE, 'w') as f:
        json.dump(read_all_plates(), f, indent=2)
    print(f"Saved all plate calibrations to {CAL_FILE}")


def restore_calibration(plate_id):
    if not os.path.exists(CAL_FILE):
        print(f"Calibration file '{CAL_FILE}' not found.")
        return

    with open(CAL_FILE) as f:
        data = json.load(f)

    if plate_id == "all":
        for plate_key in data:
            plate_num = int(plate_key.split("-")[1])
            print(f"Restoring {plate_key}...")
            write_plate_calibration(plate_num, data[plate_key])
    else:
        plate_num = int(plate_id.split("-")[1])
        if plate_id not in data:
            print(f"Plate ID {plate_id} not found in {CAL_FILE}.")
            return
        print(f"Restoring {plate_id}...")
        write_plate_calibration(plate_num, data[plate_id])


def read_calibration(plate_id):
    if plate_id == "all":
        for i in range(8):
            print(f"Plate-{i} Calibration Values:\n{get_plate_calibration(i)}\n")
    else:
        plate_num = int(plate_id.split("-")[1])
        print(f"{plate_id} Calibration Values:\n{get_plate_calibration(plate_num)}")


def main():
    parser = argparse.ArgumentParser(description="DAQC2 Calibration Tool")
    parser.add_argument("--action", choices=["save_calibration", "restore_calibration", "read_calibration"], required=True)
    parser.add_argument("--plate-id", help="Specify plate id (plate-0 to plate-7) or 'all'", default="all")
    args = parser.parse_args()

    if args.action == "save_calibration":
        save_calibration()
    elif args.action == "restore_calibration":
        restore_calibration(args.plate_id)
    elif args.action == "read_calibration":
        read_calibration(args.plate_id)


if __name__ == "__main__":
    main()
