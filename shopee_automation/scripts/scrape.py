from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    page.goto("https://shopee.co.id/search?keyword=serum wajah")

    page.wait_for_timeout(10000)

    print("Shopee kebuka")

    browser.close()