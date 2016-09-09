from .valet import Valet
from .config import Config

def create(config_file):
    config = Config(config_file)
    Valet.create_repository(config)

def print_list(config_file):
    valet = Valet(config_file)
    valet.print_all()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Describe a data repository")
    parser.add_argument("config_file", help="The path to a yaml configuration file")
    parser.add_argument("--create", action='store_true', help="Create a new repository at the given path")
    args = parser.parse_args()
    if args.create:
        create(args.config_file)
    else:
        print_list(args.config_file)


if __name__ == '__main__':
    main()