#!/usr/bin/env python3
"""
run.py
Script to run either a DaqNode or SimNode
"""

import argparse
import logging
import sys


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("NodeRunner")


def main():
    """Main function to parse arguments and run the selected node type"""
    parser = argparse.ArgumentParser(
        description="Run a measurement node (hardware or simulated)"
    )
    parser.add_argument(
        "--type",
        choices=["daq", "sim"],
        default="sim",
        help="Type of node to run (daq for hardware, sim for simulated)",
    )
    parser.add_argument("--node-id", default=None, help="Custom node ID")
    parser.add_argument(
        "--num-plates", type=int, default=2, help="Number of plates to use/simulate"
    )
    parser.add_argument("--db-path", default=None, help="Path for the buffer database")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        if args.type == "daq":
            try:
                from daq_node import DaqNode, HARDWARE_AVAILABLE

                if not HARDWARE_AVAILABLE:
                    logger.error(
                        "Hardware modules are not available. Running a SimNode instead."
                    )
                    from sim_node import SimNode

                    node_class = SimNode
                    node_type = "sim"
                else:
                    node_class = DaqNode
                    node_type = "daq"
            except ImportError as e:
                logger.error(f"Error importing DaqNode: {e}")
                logger.error("Falling back to SimNode")
                from sim_node import SimNode

                node_class = SimNode
                node_type = "sim"
        else:
            from sim_node import SimNode

            node_class = SimNode
            node_type = "sim"

        # Set default node ID and DB path based on node type
        node_id = args.node_id or f"{node_type}-001"
        db_path = args.db_path or (f"{node_type}_buffer.db")

        # Create and run the node
        logger.info(f"Starting {node_type} node with ID: {node_id}")
        node = node_class(
            node_id=node_id, num_plates=args.num_plates, buffer_db_path=db_path
        )

        # Connect to Zenoh
        node.connect_zenoh()

        # Run the node
        node.run()

    except KeyboardInterrupt:
        logger.info("Node runner stopped by user")
    except Exception as e:
        logger.error(f"Error running node: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
