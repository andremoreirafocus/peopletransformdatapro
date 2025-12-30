import time
from tasks.load.load import load
from tasks.transform.transform import transform


def main():
    start_time = time.time()
    load(year=2025, month=12, day=29, hour=15)
    transform(year=2025, month=12, day=29, hour=15)
    elapsed = time.time() - start_time
    print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
