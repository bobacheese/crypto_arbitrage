#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Program sederhana untuk menjalankan arbitrase
"""

import requests
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("run_arbitrage")

# URL API
BINANCE_REST_URL = "https://api.binance.com/api/v3"
KUCOIN_API_URL = "https://api.kucoin.com"

# Konstanta
MODAL_USD = 1000  # Modal dalam USD
MIN_PROFIT_THRESHOLD = 0.5  # Minimal persentase keuntungan
MAX_PROFIT_THRESHOLD = 100.0  # Maksimal persentase keuntungan (untuk filter false positive)
TRADING_FEE = 0.1  # Biaya trading dalam persentase

def get_binance_prices():
    """Mendapatkan harga dari Binance"""
    try:
        response = requests.get(f"{BINANCE_REST_URL}/ticker/price", timeout=10)
        data = response.json()

        prices = {}
        for item in data:
            symbol = item["symbol"]
            price = float(item["price"])
            prices[symbol] = price

        logger.info(f"Berhasil mengambil {len(prices)} harga dari Binance")
        return prices

    except Exception as e:
        logger.error(f"Error mendapatkan harga Binance: {e}")
        return {}

def get_kucoin_prices():
    """Mendapatkan harga dari KuCoin"""
    try:
        response = requests.get(f"{KUCOIN_API_URL}/api/v1/market/allTickers", timeout=10)
        data = response.json()

        if data["code"] == "200000":
            prices = {}
            for ticker in data["data"]["ticker"]:
                symbol = ticker["symbol"]
                price = float(ticker["last"])
                prices[symbol] = price

            logger.info(f"Berhasil mengambil {len(prices)} harga dari KuCoin")
            return prices
        else:
            logger.error(f"Gagal mengambil harga KuCoin: {data}")
            return {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga KuCoin: {e}")
        return {}

def normalize_binance_symbol(symbol):
    """Menormalisasi simbol Binance"""
    common_quotes = ["USDT", "BUSD", "BTC", "ETH", "BNB", "USD"]
    for quote in common_quotes:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}/{quote}"

    # Fallback
    if len(symbol) > 3:
        base = symbol[:-4]
        quote = symbol[-4:]
        return f"{base}/{quote}"

    return symbol

def normalize_kucoin_symbol(symbol):
    """Menormalisasi simbol KuCoin"""
    if "-" in symbol:
        base, quote = symbol.split("-")
        return f"{base}/{quote}"

    return symbol

def find_common_pairs(binance_prices, kucoin_prices):
    """Menemukan pasangan trading yang sama di kedua bursa"""
    normalized_binance = {normalize_binance_symbol(s): s for s in binance_prices.keys()}
    normalized_kucoin = {normalize_kucoin_symbol(s): s for s in kucoin_prices.keys()}

    common_normalized = set(normalized_binance.keys()) & set(normalized_kucoin.keys())

    common_pairs = {
        norm: {
            "binance": normalized_binance[norm],
            "kucoin": normalized_kucoin[norm]
        }
        for norm in common_normalized
    }

    logger.info(f"Ditemukan {len(common_pairs)} pasangan trading yang sama di kedua bursa")
    return common_pairs

def calculate_arbitrage(common_pairs, binance_prices, kucoin_prices):
    """Menghitung peluang arbitrase"""
    opportunities = []

    for norm_pair, exchange_pairs in common_pairs.items():
        try:
            binance_symbol = exchange_pairs["binance"]
            kucoin_symbol = exchange_pairs["kucoin"]

            binance_price = binance_prices[binance_symbol]
            kucoin_price = kucoin_prices[kucoin_symbol]

            # Lewati jika salah satu harga nol atau negatif
            if binance_price <= 0 or kucoin_price <= 0:
                continue

            # Hitung persentase perbedaan harga
            if binance_price > kucoin_price:
                price_diff_pct = ((binance_price - kucoin_price) / kucoin_price) * 100
                buy_exchange = "kucoin"
                sell_exchange = "binance"
                buy_price = kucoin_price
                sell_price = binance_price
            else:
                price_diff_pct = ((kucoin_price - binance_price) / binance_price) * 100
                buy_exchange = "binance"
                sell_exchange = "kucoin"
                buy_price = binance_price
                sell_price = kucoin_price
        except Exception as e:
            logger.warning(f"Error menghitung arbitrase untuk {norm_pair}: {e}")
            continue

        # Jika perbedaan harga terlalu kecil, lewati
        if price_diff_pct < MIN_PROFIT_THRESHOLD:
            continue

        # Hitung jumlah yang bisa dibeli dengan modal
        quantity = MODAL_USD / buy_price

        # Hitung biaya trading
        buy_fee = (quantity * buy_price) * (TRADING_FEE / 100)
        sell_fee = (quantity * sell_price) * (TRADING_FEE / 100)

        # Hitung nilai setelah jual
        sell_value = (quantity * sell_price) - sell_fee

        # Hitung keuntungan kotor (dalam USD)
        gross_profit_usd = sell_value - (quantity * buy_price) - buy_fee

        # Hitung ROI
        roi = (gross_profit_usd / MODAL_USD) * 100

        # Jika menguntungkan dan ROI masuk akal (untuk filter false positive)
        if gross_profit_usd > 0 and roi <= MAX_PROFIT_THRESHOLD:
            opportunity = {
                "pair": norm_pair,
                "binance_symbol": binance_symbol,
                "kucoin_symbol": kucoin_symbol,
                "binance_price": binance_price,
                "kucoin_price": kucoin_price,
                "price_diff_pct": price_diff_pct,
                "buy_exchange": buy_exchange,
                "sell_exchange": sell_exchange,
                "gross_profit_usd": gross_profit_usd,
                "roi": roi,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            opportunities.append(opportunity)

            logger.info(
                f"Peluang arbitrase ditemukan: {norm_pair} - "
                f"Beli di {buy_exchange.upper()} ({buy_price:.8f}), "
                f"Jual di {sell_exchange.upper()} ({sell_price:.8f}), "
                f"Profit: ${gross_profit_usd:.2f}, ROI: {roi:.2f}%"
            )

    # Urutkan berdasarkan keuntungan (tertinggi ke terendah)
    opportunities.sort(key=lambda x: x["gross_profit_usd"], reverse=True)

    return opportunities

def display_opportunities(opportunities):
    """Menampilkan peluang arbitrase"""
    print("\n=== TOP 5 PELUANG ARBITRASE ===")
    print(f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    if not opportunities:
        print("Tidak ada peluang arbitrase ditemukan")
        return

    for i, opp in enumerate(opportunities[:5], 1):
        print(f"{i}. {opp['pair']}")
        if opp['buy_exchange'] == 'binance':
            buy_price = opp['binance_price']
            sell_price = opp['kucoin_price']
        else:
            buy_price = opp['kucoin_price']
            sell_price = opp['binance_price']

        print(f"   Beli di: {opp['buy_exchange'].upper()} dengan harga {buy_price:.8f}")
        print(f"   Jual di: {opp['sell_exchange'].upper()} dengan harga {sell_price:.8f}")
        print(f"   Selisih: {opp['price_diff_pct']:.2f}%")
        print(f"   Profit: ${opp['gross_profit_usd']:.2f}")
        print(f"   ROI: {opp['roi']:.2f}%")
        print("-" * 80)

def main():
    """Fungsi utama"""
    print("Memulai Crypto Arbitrage Scanner...")

    try:
        # Dapatkan harga dari kedua bursa
        binance_prices = get_binance_prices()
        kucoin_prices = get_kucoin_prices()

        if not binance_prices or not kucoin_prices:
            print("Gagal mendapatkan harga dari salah satu bursa")
            return

        # Temukan pasangan trading yang sama
        common_pairs = find_common_pairs(binance_prices, kucoin_prices)

        # Hitung peluang arbitrase
        opportunities = calculate_arbitrage(common_pairs, binance_prices, kucoin_prices)

        # Tampilkan peluang
        display_opportunities(opportunities)

        print("\nProgram selesai")

    except Exception as e:
        logger.exception(f"Error tidak tertangani: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
