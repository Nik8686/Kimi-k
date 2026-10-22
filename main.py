import os
import requests
import time
from datetime import datetime

# جلب الإعدادات من متغيرات البيئة في Railway
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

class PumpFunRailwayBot:
    def __init__(self):
        self.api_url = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://pump.fun",
            "Referer": "https://pump.fun/"
        }
        self.processed_mints = set()

    def send_telegram_alert(self, token_name, symbol, mint, holders, usd_mcap):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print(f"⚠️ التنبيه محلي (تيليجرام غير مضبوط): {token_name} | المحافظ: {holders}")
            return
        
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        message = (
            f"🚀 **عملة جديدة مطابقة للفلتر!**\n\n"
            f"📌 **الاسم:** {token_name} ({symbol})\n"
            f"🔑 **العقد (Mint):** `{mint}`\n\n"
            f"📊 **المؤشرات الحالية:**\n"
            f"• المحافظ الفريدة: {holders} (≥ 50)\n"
            f"• القيمة السوقية التقريبية: `${usd_mcap:,.2f}`\n"
        )

        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "⚡ Photon", "url": f"https://photon-sol.tinyastro.io/en/token/{mint}"},
                    {"text": "🔍 Solscan", "url": f"https://solscan.io/token/{mint}"}
                ]
            ]
        }

        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "reply_markup": reply_markup
        }

        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"خطأ في إرسال تيليجرام: {e}")

    def run(self):
        print("🤖 بدء تشغيل رادار Pump.fun على Railway...")
        print("=" * 60)

        while True:
            try:
                response = requests.get(self.api_url, headers=self.headers, timeout=15)
                if response.status_code == 200:
                    tokens = response.json()
                    if isinstance(tokens, list):
                        for token in tokens:
                            mint = token.get("mint")
                            if not mint or mint in self.processed_mints:
                                continue

                            name = token.get("name", "Unknown")
                            symbol = token.get("symbol", "???")
                            holders = token.get("holder_count", 0)
                            
                            virtual_sol = token.get("virtual_sol_reserves", 0) / 1e9
                            usd_mcap = virtual_sol * 200 * 15

                            if holders >= 50:
                                self.processed_mints.add(mint)
                                print(f"✅ عملة مطابقة: {name} ({symbol}) | المحافظ: {holders} | MCap: ${usd_mcap:,.2f}")
                                self.send_telegram_alert(name, symbol, mint, holders, usd_mcap)

            except Exception as e:
                print(f"⚠️ خطأ أثناء الجلب: {e}")

            time.sleep(15)

if __name__ == "__main__":
    bot = PumpFunRailwayBot()
    bot.run()
