from frequenz.client.reporting.component_graph import ComponentGraph, Component, Connection, ComponentCategory, InverterType
import json


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


def main():
    # Read JSON data from file
    with open("comps13.json", "r") as file:
        data = json.load(file)

    # Build component graph
    component_graph = build_graph(data)

    # Print component graph
    print(component_graph)

if __name__ == "__main__":
    main()
