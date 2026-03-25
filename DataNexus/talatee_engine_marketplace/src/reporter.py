def save_report(df, path):
    df.to_csv(path, index=False)