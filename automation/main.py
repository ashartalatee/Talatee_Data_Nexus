# main.py

from core.orchestrator import run_pipeline


def main():
    print("🚀 Talatee Automation Engine Started")

    try:
        run_pipeline()
        print("✅ Pipeline completed successfully")

    except Exception as e:
        print(f"❌ Error occurred: {e}")


if __name__ == "__main__":
    main()