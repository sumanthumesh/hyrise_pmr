import argparse
import json


def main() -> None:
    parser = argparse.ArgumentParser(description="Sort placement column lists alphabetically in mapping JSON files (in-place).")
    parser.add_argument("files", nargs="+", help="Mapping JSON files to sort")
    args = parser.parse_args()

    for path in args.files:
        with open(path) as f:
            data = json.load(f)
        data["placement"] = [sorted(mem) for mem in data["placement"]]
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"sorted: {path}")


if __name__ == "__main__":
    main()
