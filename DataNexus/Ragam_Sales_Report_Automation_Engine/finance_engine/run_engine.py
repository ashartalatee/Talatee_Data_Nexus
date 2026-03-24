from engine.loader import load_data

def main():
    print("🚀 Finance Engine Starting...")

    df = load_data()

    print("\n📊 Preview Combined Data:")
    print(df.head())

if __name__ == "__main__":
    main()