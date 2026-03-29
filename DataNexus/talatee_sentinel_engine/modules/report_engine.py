from datetime import datetime

def generate_report(insights):
    today = datetime.now().strftime("%Y-%m-%d")

    report = []
    report.append("="*50)
    report.append(f"TALATEE BUSINESS REPORT - {today}")
    report.append("="*50)
    report.append("\n📊 EXECUTIVE SUMMARY:\n")

    for ins in insights:
        report.append(f"- {ins}")

    report.append("\n" + "="*50)
    report.append("END OF REPORT")
    report.append("="*50)

    return "\n".join(report)


def save_report(report_text):
    with open("outputs/report/final_report.txt", "w") as f:
        f.write(report_text)

    print("📄 Final report ready!")