#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crypto Arbitrage Scanner - Main Program
---------------------------------------
Program utama untuk menjalankan Crypto Arbitrage Scanner
"""

import asyncio
import threading
import signal
import sys
import logging
import argparse
import time
from datetime import datetime

# Tambahkan print untuk debugging
print("Program dimulai...")

# Import modul-modul
try:
    print("Mengimpor config...")
    from config import (
        BINANCE_WS_URL, BINANCE_REST_URL, KUCOIN_API_URL,
        MODAL_IDR, DEFAULT_IDR_USD_RATE
    )

    print("Mengimpor utils...")
    from utils import setup_logging, get_exchange_rate

    print("Mengimpor exchange...")
    from exchange import BinanceExchange, KuCoinExchange

    print("Mengimpor arbitrage...")
    from arbitrage import ArbitrageDetector

    print("Mengimpor ui...")
    from ui import ArbitrageUI, LoadingSpinner

    print("Semua modul berhasil diimpor")
except Exception as e:
    print(f"Error mengimpor modul: {e}")
    sys.exit(1)

# Setup argparse
parser = argparse.ArgumentParser(description="Crypto Arbitrage Scanner")
parser.add_argument("--debug", action="store_true", help="Enable debug logging")
parser.add_argument("--no-ui", action="store_true", help="Run without UI (console mode)")
parser.add_argument("--log-file", default="arbitrage.log", help="Log file path")
args = parser.parse_args()

# Setup logging
log_level = logging.DEBUG if args.debug else logging.INFO
logger = setup_logging(args.log_file, log_level, logging.DEBUG)

# Variabel global
running = True
exchanges_ready = False
arbitrage_thread = None
ui_thread = None

def signal_handler(sig, frame):
    """Menangani sinyal interupsi"""
    global running
    logger.info("Menerima sinyal interupsi, menutup program...")
    running = False
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

async def update_arbitrage_loop(arbitrage_detector, update_interval=10):
    """Loop untuk memperbarui peluang arbitrase"""
    global running, exchanges_ready

    while running:
        try:
            # Tunggu hingga kedua bursa siap
            if not exchanges_ready:
                await asyncio.sleep(1)
                continue

            # Update peluang arbitrase
            arbitrage_detector.update()

            # Tunggu sebelum update berikutnya
            await asyncio.sleep(update_interval)

        except Exception as e:
            logger.error(f"Error dalam update arbitrage loop: {e}")
            await asyncio.sleep(5)

async def main():
    """Fungsi utama program"""
    global running, exchanges_ready, arbitrage_thread, ui_thread

    try:
        logger.info("Memulai Crypto Arbitrage Scanner...")

        # Dapatkan kurs IDR/USD
        idr_usd_rate = get_exchange_rate("USD", "IDR")
        if idr_usd_rate <= 0:
            idr_usd_rate = DEFAULT_IDR_USD_RATE
            logger.warning(f"Gagal mendapatkan kurs IDR/USD, menggunakan nilai default: {idr_usd_rate}")
        else:
            logger.info(f"Kurs IDR/USD: {idr_usd_rate:,.2f}")

        # Hitung modal dalam USD
        modal_usd = MODAL_IDR / idr_usd_rate
        logger.info(f"Modal: Rp {MODAL_IDR:,.2f} (${modal_usd:,.2f})")

        # Inisialisasi bursa
        binance = BinanceExchange(BINANCE_WS_URL, BINANCE_REST_URL)
        kucoin = KuCoinExchange(KUCOIN_API_URL)

        # Inisialisasi detektor arbitrase
        arbitrage_detector = ArbitrageDetector(binance, kucoin, modal_usd)

        # Set callback untuk update harga
        def on_price_update():
            if exchanges_ready:
                # Jalankan update dalam thread terpisah untuk menghindari blocking
                threading.Thread(target=arbitrage_detector.update).start()

        binance.set_price_update_callback(on_price_update)
        kucoin.set_price_update_callback(on_price_update)

        # Tampilkan spinner loading
        with LoadingSpinner() as spinner:
            # Hubungkan ke bursa dalam task terpisah
            spinner.update("Menghubungkan ke Binance...")
            binance_task = asyncio.create_task(binance.connect())

            # Tunggu sebentar
            await asyncio.sleep(2)

            spinner.update("Menghubungkan ke KuCoin...")
            kucoin_task = asyncio.create_task(kucoin.connect())

            # Tunggu hingga kedua bursa terhubung
            spinner.update("Menunggu koneksi ke bursa...")
            await asyncio.sleep(5)

            # Temukan pasangan umum
            spinner.update("Mencari pasangan trading yang sama...")
            arbitrage_detector.find_common_pairs()

            # Hitung peluang arbitrase pertama kali
            spinner.update("Menghitung peluang arbitrase...")
            arbitrage_detector.calculate_arbitrage()

            # Set flag bahwa bursa sudah siap
            exchanges_ready = True

        # Mulai loop update arbitrase dalam task terpisah
        arbitrage_task = asyncio.create_task(
            update_arbitrage_loop(arbitrage_detector)
        )

        # Inisialisasi dan mulai UI jika tidak dalam mode no-ui
        if not args.no_ui:
            # Inisialisasi UI
            ui = ArbitrageUI(binance, kucoin, arbitrage_detector, idr_usd_rate)

            # Jalankan UI dalam thread terpisah
            ui_thread = threading.Thread(target=ui.start)
            ui_thread.daemon = True
            ui_thread.start()

        # Loop utama
        while running:
            # Tampilkan peluang arbitrase jika dalam mode no-ui
            if args.no_ui:
                opportunities = arbitrage_detector.get_opportunities()
                if opportunities:
                    logger.info(f"Ditemukan {len(opportunities)} peluang arbitrase:")
                    for i, opp in enumerate(opportunities[:5], 1):
                        logger.info(
                            f"{i}. {opp['pair']} - "
                            f"Beli di {opp['buy_exchange'].upper()}, "
                            f"Jual di {opp['sell_exchange'].upper()}, "
                            f"Profit: ${opp['net_profit_usd']:.2f}, "
                            f"ROI: {opp['roi']:.2f}%"
                        )
                    logger.info(f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info("=" * 80)

            # Tunggu sebelum iterasi berikutnya
            await asyncio.sleep(10)

        # Tunggu hingga semua task selesai
        await asyncio.gather(binance_task, kucoin_task, arbitrage_task)

    except KeyboardInterrupt:
        logger.info("Program dihentikan oleh pengguna")
        running = False

    except Exception as e:
        logger.exception(f"Error tidak tertangani: {e}")

    finally:
        # Tutup koneksi ke bursa
        logger.info("Menutup koneksi ke bursa...")
        await binance.disconnect()
        await kucoin.disconnect()

        # Tunggu thread UI selesai jika ada
        if ui_thread and ui_thread.is_alive():
            ui_thread.join(timeout=1)

        logger.info("Program selesai")

if __name__ == "__main__":
    try:
        # Jalankan loop asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program dihentikan oleh pengguna")
    except Exception as e:
        logger.exception(f"Error tidak tertangani: {e}")
