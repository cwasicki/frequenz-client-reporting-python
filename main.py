from frequenz.client.reporting.component_graph import ComponentGraph, Component, Connection, ComponentCategory, InverterType
import json
from frequenz.client.reporting import ReportingApiClient
from frequenz.client.reporting.sdk_reporting_bridge import list_microgrid_components_data_receiver
import asyncio
from datetime import datetime

from frequenz.client.common.metric import Metric
def build_graph(json_data: dict) -> ComponentGraph:
    components = []
    connections = []
    for component in json_data["components"]:
        component_id = int(component["id"])
        category = ComponentCategory(component["category"])
        component_type = None
        if category == ComponentCategory.INVERTER and "inverter" in component and "type" in component["inverter"]:
            component_type = InverterType(component["inverter"]["type"])

        components.append(
            Component(
                component_id=component_id,
                category=category,
                type=component_type,
            )
        )
    for connection in json_data["connections"]:
        connections.append(
            Connection(
                start=int(connection["start"]),
                end=int(connection["end"]),
            )
        )
    return ComponentGraph(components, connections)


async def main():
    # Read JSON data from file
    with open("comps13.json", "r") as file:
        data = json.load(file)

    # Build component graph
    component_graph = build_graph(data)

    # Print component graph
    print(component_graph)

    key = open("key.txt", "r").read().strip()
    client = ReportingApiClient(server_url="grpc://reporting.api.frequenz.com:443?ssl=true", key=key)

    microgrid_id = 13
    component_ids = [256, 258]
    microgrid_components = [
        (microgrid_id, component_ids),
    ]
    start_dt = datetime(2024, 9, 17)
    end_dt = datetime(2024, 9, 18)
    resolution = 900

    async for sample in list_microgrid_components_data_receiver(
        client,
        microgrid_components=microgrid_components,
        metrics=[Metric.AC_ACTIVE_POWER],
        start_dt=start_dt,
        end_dt=end_dt,
        resolution=resolution,
    ):
        print(sample)

if __name__ == "__main__":
    asyncio.run(main())
