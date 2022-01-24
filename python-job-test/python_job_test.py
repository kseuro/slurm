from argparse import ArgumentParser

def main():
    parser = ArgumentParser(description="Generic ArgParser")
    parser.add_argument("--arg1", type=str, default="argument1")
    config = vars(parser.parse_args())

    try:
        import scanpy as sc
        print("Imported scanpy")
    except:
        print("Failed to import scanpy")
    
    print(f"Arg1: {config['arg1']}")
    print("Done...")

if __name__ == "__main__":
    main()