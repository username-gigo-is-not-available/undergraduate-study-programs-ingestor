from src.models.enums import ComponentType, ComponentName


class Component:

    def __init__(self, component_name: ComponentName, component_type: ComponentType):
        self.component_name = component_name
        self.component_type = component_type

    def __str__(self):
        return f"{self.component_name.value} ({self.component_type.value})"
