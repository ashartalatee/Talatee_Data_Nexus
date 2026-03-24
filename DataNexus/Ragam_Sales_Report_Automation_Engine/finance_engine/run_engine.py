from engine.loader import load_data

def main():
    print("🚀 Finance Engine Starting...")

    df_shopee, df_tokopedia = load_data()

    print("\n📊 Preview Shopee:")
    print(df_shopee.head())

    print("\n📊 Preview Tokopedia:")
    print(df_tokopedia.head())

if __name__ == "__main__":
    main()