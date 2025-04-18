#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utilitas untuk program Crypto Arbitrage Scanner
"""

import requests
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

logger = logging.getLogger("crypto_arbitrage.utils")

def format_currency(amount: float, currency: str = "IDR", precision: int = 2) -> str:
    """
    Format angka sebagai mata uang

    Args:
        amount: Jumlah yang akan diformat
        currency: Kode mata uang (IDR, USD, dll)
        precision: Jumlah angka di belakang koma

    Returns:
        String yang diformat sebagai mata uang
    """
    if currency == "IDR":
        return f"Rp {amount:,.{precision}f}"
    elif currency == "USD":
        return f"${amount:,.{precision}f}"
    else:
        return f"{currency} {amount:,.{precision}f}"

def get_exchange_rate(from_currency: str, to_currency: str) -> float:
    """
    Mendapatkan kurs mata uang dari API

    Args:
        from_currency: Mata uang asal
        to_currency: Mata uang tujuan

    Returns:
        Nilai kurs
    """
    try:
        response = requests.get(f"https://api.exchangerate-api.com/v4/latest/{from_currency}")
        data = response.json()
        return data["rates"][to_currency]
    except Exception as e:
        logger.error(f"Error mendapatkan kurs {from_currency}/{to_currency}: {e}")
        # Nilai default jika API tidak tersedia
        if from_currency == "USD" and to_currency == "IDR":
            return 15000
        elif from_currency == "IDR" and to_currency == "USD":
            return 1/15000
        return 1.0

def calculate_profit(
    buy_price: float,
    sell_price: float,
    quantity: float,
    buy_fee_pct: float,
    sell_fee_pct: float,
    withdrawal_fee: float = 0
) -> Dict[str, float]:
    """
    Menghitung keuntungan dari arbitrase

    Args:
        buy_price: Harga beli
        sell_price: Harga jual
        quantity: Jumlah yang dibeli/dijual
        buy_fee_pct: Persentase biaya pembelian
        sell_fee_pct: Persentase biaya penjualan
        withdrawal_fee: Biaya penarikan (dalam mata uang dasar)

    Returns:
        Dictionary berisi keuntungan kotor dan bersih
    """
    # Biaya trading
    buy_fee = (quantity * buy_price) * (buy_fee_pct / 100)
    sell_fee = (quantity * sell_price) * (sell_fee_pct / 100)

    # Keuntungan kotor (tanpa biaya penarikan)
    gross_profit = (quantity * sell_price) - (quantity * buy_price) - buy_fee - sell_fee

    # Keuntungan bersih (dengan biaya penarikan)
    net_profit = gross_profit - withdrawal_fee

    # ROI
    roi = (net_profit / (quantity * buy_price)) * 100

    return {
        "gross_profit": gross_profit,
        "net_profit": net_profit,
        "roi": roi
    }

def find_common_networks(
    asset: str,
    binance_networks: Dict[str, List[str]],
    kucoin_networks: Dict[str, List[str]]
) -> List[str]:
    """
    Menemukan jaringan yang didukung oleh kedua bursa untuk suatu aset

    Args:
        asset: Simbol aset (BTC, ETH, dll)
        binance_networks: Dictionary jaringan Binance
        kucoin_networks: Dictionary jaringan KuCoin

    Returns:
        List jaringan yang didukung oleh kedua bursa
    """
    if asset not in binance_networks or asset not in kucoin_networks:
        return []

    binance_asset_networks = set(binance_networks[asset])
    kucoin_asset_networks = set(kucoin_networks[asset])

    return list(binance_asset_networks & kucoin_asset_networks)

def normalize_symbol(symbol: str, exchange: str) -> str:
    """
    Menormalisasi nama simbol untuk konsistensi antar bursa

    Args:
        symbol: Simbol yang akan dinormalisasi
        exchange: Nama bursa (binance, kucoin)

    Returns:
        Simbol yang dinormalisasi
    """
    if exchange == "binance":
        # Binance format: BTCUSDT
        common_quotes = ["USDT", "BUSD", "BTC", "ETH", "BNB", "USD"]
        for quote in common_quotes:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return f"{base}/{quote}"

        # Fallback jika tidak ada quote yang cocok
        if len(symbol) > 3:
            base = symbol[:-4]
            quote = symbol[-4:]
            return f"{base}/{quote}"

    elif exchange == "kucoin":
        # KuCoin format: BTC-USDT
        if "-" in symbol:
            base, quote = symbol.split("-")
            return f"{base}/{quote}"

    # Jika tidak bisa dinormalisasi, kembalikan simbol asli
    return symbol

def extract_base_quote(normalized_symbol: str) -> Tuple[str, str]:
    """
    Mengekstrak base dan quote asset dari simbol yang dinormalisasi

    Args:
        normalized_symbol: Simbol yang sudah dinormalisasi (format: BASE/QUOTE)

    Returns:
        Tuple (base_asset, quote_asset)
    """
    if "/" in normalized_symbol:
        base, quote = normalized_symbol.split("/")
        return base, quote

    # Fallback jika format tidak sesuai
    return normalized_symbol, ""

def get_min_withdrawal_fee_network(
    asset: str,
    networks: List[str],
    withdrawal_fees: Dict[str, Dict[str, Dict[str, float]]]
) -> Tuple[str, float]:
    """
    Mendapatkan jaringan dengan biaya penarikan terendah

    Args:
        asset: Simbol aset
        networks: List jaringan yang tersedia
        withdrawal_fees: Dictionary biaya penarikan

    Returns:
        Tuple (jaringan_terbaik, biaya_penarikan)
    """
    if not networks or asset not in withdrawal_fees:
        return "", 0

    min_fee = float('inf')
    best_network = networks[0]

    for network in networks:
        for exchange in withdrawal_fees[asset]:
            if network in withdrawal_fees[asset][exchange]:
                fee = withdrawal_fees[asset][exchange][network]
                if fee < min_fee:
                    min_fee = fee
                    best_network = network

    return best_network, min_fee


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Konversi nilai ke float dengan aman

    Args:
        value: Nilai yang akan dikonversi
        default: Nilai default jika konversi gagal

    Returns:
        Nilai float
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def calculate_slippage(price: float, volume: float, side: str, slippage_factor: float = 0.001) -> float:
    """
    Menghitung estimasi slippage berdasarkan volume

    Args:
        price: Harga aset
        volume: Volume 24 jam dalam USD
        side: 'buy' atau 'sell'
        slippage_factor: Faktor slippage (default 0.1%)

    Returns:
        Estimasi harga setelah slippage
    """
    # Semakin tinggi volume, semakin rendah slippage
    volume_factor = min(1.0, 100000 / max(volume, 1))

    # Slippage lebih tinggi untuk pembelian
    if side == "buy":
        return price * (1 + slippage_factor * volume_factor)
    else:
        return price * (1 - slippage_factor * volume_factor)


def exponential_backoff(retry_count: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Menghitung delay untuk exponential backoff

    Args:
        retry_count: Jumlah percobaan yang telah dilakukan
        base_delay: Delay dasar dalam detik
        max_delay: Delay maksimum dalam detik

    Returns:
        Delay dalam detik
    """
    delay = min(max_delay, base_delay * (2 ** retry_count))
    # Tambahkan jitter untuk menghindari thundering herd
    jitter = delay * 0.1 * (time.time() % 1)
    return delay + jitter


def validate_arbitrage_opportunity(opportunity: Dict[str, Any], max_roi: float = 100.0) -> Tuple[bool, str]:
    """
    Memvalidasi peluang arbitrase

    Args:
        opportunity: Data peluang arbitrase
        max_roi: ROI maksimum yang dianggap valid

    Returns:
        (valid, alasan)
    """
    # Cek ROI yang tidak realistis
    if opportunity["roi"] > max_roi:
        return False, f"ROI terlalu tinggi ({opportunity['roi']:.2f}%)"

    # Cek harga negatif
    if opportunity["binance_price"] <= 0 or opportunity["kucoin_price"] <= 0:
        return False, "Harga tidak valid"

    # Cek profit negatif
    if opportunity["net_profit_usd"] <= 0:
        return False, "Profit negatif"

    # Cek timestamp (tidak lebih dari 5 menit)
    opp_time = datetime.strptime(opportunity["timestamp"], "%Y-%m-%d %H:%M:%S")
    if (datetime.now() - opp_time) > timedelta(minutes=5):
        return False, "Data sudah kedaluwarsa"

    return True, "Valid"


def calculate_accurate_slippage(order_book: Dict, quantity: float, side: str) -> float:
    """
    Menghitung slippage berdasarkan order book

    Args:
        order_book: Order book (bids/asks)
        quantity: Jumlah yang akan dibeli/dijual
        side: 'buy' atau 'sell'

    Returns:
        Harga rata-rata setelah slippage
    """
    if not order_book or not quantity:
        return 0.0

    orders = order_book["bids"] if side == "sell" else order_book["asks"]

    if not orders:
        return 0.0

    total_quantity = 0.0
    total_value = 0.0

    for price, qty in orders:
        price = safe_float(price)
        qty = safe_float(qty)

        if total_quantity + qty >= quantity:
            # Hanya ambil sebagian dari level harga ini
            remaining = quantity - total_quantity
            total_value += price * remaining
            total_quantity += remaining
            break
        else:
            # Ambil semua dari level harga ini
            total_value += price * qty
            total_quantity += qty

    # Jika tidak cukup likuiditas di order book
    if total_quantity < quantity:
        return 0.0

    # Harga rata-rata
    return total_value / total_quantity


def is_opportunity_expired(timestamp: str, max_age_seconds: int = 300) -> bool:
    """
    Memeriksa apakah peluang arbitrase sudah kedaluwarsa

    Args:
        timestamp: Timestamp dalam format '%Y-%m-%d %H:%M:%S'
        max_age_seconds: Maksimal umur peluang dalam detik

    Returns:
        True jika sudah kedaluwarsa, False jika belum
    """
    try:
        opp_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        age_seconds = (datetime.now() - opp_time).total_seconds()
        return age_seconds > max_age_seconds
    except Exception:
        # Jika format timestamp tidak valid, anggap sudah kedaluwarsa
        return True


def validate_price_data(price: float, min_price: float = 0.0) -> bool:
    """
    Memvalidasi data harga

    Args:
        price: Harga yang akan divalidasi
        min_price: Harga minimal yang dianggap valid

    Returns:
        True jika valid, False jika tidak
    """
    return isinstance(price, (int, float)) and price > min_price


def setup_logging(log_file: str = "arbitrage.log", console_level: int = logging.INFO, file_level: int = logging.DEBUG) -> logging.Logger:
    """
    Mengatur konfigurasi logging

    Args:
        log_file: Nama file log
        console_level: Level logging untuk console
        file_level: Level logging untuk file

    Returns:
        Logger yang telah dikonfigurasi
    """
    logger = logging.getLogger("crypto_arbitrage")
    logger.setLevel(logging.DEBUG)

    # Bersihkan handler yang ada
    if logger.handlers:
        logger.handlers.clear()

    # Handler untuk file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(file_level)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Handler untuk console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)

    # Tambahkan handler ke logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
