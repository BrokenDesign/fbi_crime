import srsly

layout = srsly.read_yaml("layouts/2021 Hate Crime Master Layout.yaml")


def print_variable_list():
    for _, record in layout.items():  # type: ignore
        print("*" * 50)
        for variable in record:
            print("{}: {}".format(variable["name"], variable["type"]))


def print_position_list():
    for _, record in layout.items():  # type: ignore
        print("*" * 50)
        for variable in record:
            print("{}: {}".format(variable["name"], variable["type"]))
