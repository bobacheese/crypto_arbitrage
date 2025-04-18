#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Tambahkan print untuk debugging
print("Test exchange starting...")

"""
Program untuk menguji koneksi ke bursa
"""

import asyncio
import logging
from utils import setup_logging
from config import BINANCE_WS_URL, BINANCE_REST_URL, KUCOIN_API_URL
from exchange import BinanceExchange, KuCoinExchange

# Setup logging
logger = setup_logging("test_exchange.log", logging.INFO, logging.DEBUG)

async def main():
    """Fungsi utama"""
    print("Memulai pengujian koneksi ke bursa...")

    try:
        # Inisialisasi bursa
        print("Inisialisasi Binance...")
        binance = BinanceExchange(BINANCE_WS_URL, BINANCE_REST_URL)

        print("Inisialisasi KuCoin...")
        kucoin = KuCoinExchange(KUCOIN_API_URL)

        # Hubungkan ke Binance
        print("Menghubungkan ke Binance...")
        binance_task = asyncio.create_task(binance.connect())

        # Tunggu sebentar
        await asyncio.sleep(5)

        # Hubungkan ke KuCoin
        print("Menghubungkan ke KuCoin...")
        kucoin_task = asyncio.create_task(kucoin.connect())

        # Tunggu sebentar
        await asyncio.sleep(10)

        # Tampilkan informasi
        print(f"Jumlah simbol Binance: {len(binance.symbols)}")
        print(f"Jumlah simbol KuCoin: {len(kucoin.symbols)}")

        # Tampilkan beberapa harga
        if binance.symbols:
            symbol = next(iter(binance.symbols))
            price = binance.get_price(symbol)
            print(f"Harga Binance {symbol}: {price}")

        if kucoin.symbols:
            symbol = next(iter(kucoin.symbols))
            price = kucoin.get_price(symbol)
            print(f"Harga KuCoin {symbol}: {price}")

        # Tunggu sebentar
        print("Menunggu data...")
        await asyncio.sleep(10)

        # Tampilkan informasi lagi
        print(f"Jumlah simbol Binance: {len(binance.symbols)}")
        print(f"Jumlah simbol KuCoin: {len(kucoin.symbols)}")

        # Tutup koneksi
        print("Menutup koneksi...")
        await binance.disconnect()
        await kucoin.disconnect()

        print("Pengujian selesai")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Jalankan loop asyncio
    asyncio.run(main())
