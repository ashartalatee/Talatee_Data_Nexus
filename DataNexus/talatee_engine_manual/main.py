import argparse
from src.orchestrator.manager import run_all_clients

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Talatee Engine Runner")
    parser.add_argument("--client", type=str, help="Run specific client")

    args = parser.parse_args()

    try:
        print("[START] Running engine...")

        if args.client:
            print(f"[INFO] Target client: {args.client}")
            run_all_clients(target_client=args.client)
        else:
            run_all_clients()

        print("[DONE] All clients executed successfully.")

    except Exception as e:
        print(f"[FATAL ERROR] Engine failed: {e}")