#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Final Crypto Arbitrage Scanner
------------------------------
Program untuk mendeteksi peluang arbitrase antara Binance dan KuCoin
dengan validasi dan perhitungan yang akurat.
"""

import asyncio
import logging
import signal
import sys
import time
import json
import requests
from datetime import datetime
from typing import Dict, List, Tuple, Any

# Konfigurasi
BINANCE_API_URL = "https://api.binance.com/api/v3"
KUCOIN_API_URL = "https://api.kucoin.com"
BYBIT_API_URL = "https://api.bybit.com/v5"
OKX_API_URL = "https://www.okx.com/api/v5"
GATE_API_URL = "https://api.gateio.ws/api/v4"
MEXC_API_URL = "https://api.mexc.com/api/v3"
HTX_API_URL = "https://api.huobi.pro/market"
MODAL_USD = 1000  # Modal dalam USD
MIN_PROFIT_THRESHOLD = 0.5  # Minimal persentase keuntungan
MAX_PROFIT_THRESHOLD = 100.0  # Maksimal persentase keuntungan
MIN_VOLUME_USD = 100000  # Minimal volume 24 jam dalam USD
UPDATE_INTERVAL = 60  # Interval update dalam detik

# Biaya trading (dalam persentase)
TRADING_FEES = {
    "binance": {
        "maker": 0.1,  # Biaya maker (limit order)
        "taker": 0.1,  # Biaya taker (market order)
        "default": 0.1  # Biaya default
    },
    "kucoin": {
        "maker": 0.1,  # Biaya maker (limit order)
        "taker": 0.1,  # Biaya taker (market order)
        "default": 0.1  # Biaya default
    },
    "bybit": {
        "maker": 0.1,  # Biaya maker (limit order)
        "taker": 0.1,  # Biaya taker (market order)
        "default": 0.1  # Biaya default
    },
    "okx": {
        "maker": 0.1,  # Biaya maker (limit order)
        "taker": 0.1,  # Biaya taker (market order)
        "default": 0.1  # Biaya default
    },
    "gate": {
        "maker": 0.2,  # Biaya maker (limit order)
        "taker": 0.2,  # Biaya taker (market order)
        "default": 0.2  # Biaya default
    },
    "mexc": {
        "maker": 0.2,  # Biaya maker (limit order)
        "taker": 0.2,  # Biaya taker (market order)
        "default": 0.2  # Biaya default
    },
    "htx": {
        "maker": 0.2,  # Biaya maker (limit order)
        "taker": 0.2,  # Biaya taker (market order)
        "default": 0.2  # Biaya default
    }
}

# Biaya penarikan (dalam USD atau dalam token)
# Nilai negatif berarti biaya dalam token, nilai positif berarti biaya dalam USD
WITHDRAWAL_FEES = {
    "BTC": {
        "BTC": -0.0005,  # 0.0005 BTC
        "BEP20": -0.0000005,  # 0.0000005 BTC
        "ERC20": -0.0005  # 0.0005 BTC
    },
    "ETH": {
        "ETH": -0.005,  # 0.005 ETH
        "BEP20": -0.0001,  # 0.0001 ETH
        "ARBITRUM": -0.0001,  # 0.0001 ETH
        "OPTIMISM": -0.0001  # 0.0001 ETH
    },
    "USDT": {
        "ERC20": 15.0,  # $15 USD
        "TRC20": 1.0,  # $1 USD
        "BEP20": 1.0,  # $1 USD
        "SOL": 1.0,  # $1 USD
        "MATIC": 0.8,  # $0.8 USD
        "ARBITRUM": 0.8,  # $0.8 USD
        "OPTIMISM": 0.8,  # $0.8 USD
        "AVAX": 0.8  # $0.8 USD
    },
    "default": {
        "ERC20": 15.0,  # $15 USD
        "TRC20": 1.0,  # $1 USD
        "BEP20": 1.0,  # $1 USD
        "default": 5.0  # $5 USD
    }
}

# Biaya gas untuk jaringan (dalam USD)
GAS_FEES = {
    "ERC20": 5.0,  # $5 USD
    "TRC20": 0.5,  # $0.5 USD
    "BEP20": 0.3,  # $0.3 USD
    "SOL": 0.01,  # $0.01 USD
    "MATIC": 0.1,  # $0.1 USD
    "ARBITRUM": 0.3,  # $0.3 USD
    "OPTIMISM": 0.2,  # $0.2 USD
    "AVAX": 0.2,  # $0.2 USD
    "default": 1.0  # $1 USD
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("final_arbitrage.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("final_arbitrage")

# Variabel global
running = True

# Cache untuk status trading dan jaringan
binance_trading_status_cache = {}
kucoin_trading_status_cache = {}
bybit_trading_status_cache = {}
okx_trading_status_cache = {}
gate_trading_status_cache = {}
mexc_trading_status_cache = {}
htx_trading_status_cache = {}
binance_networks_cache = {}
kucoin_networks_cache = {}
bybit_networks_cache = {}
okx_networks_cache = {}
gate_networks_cache = {}
mexc_networks_cache = {}
htx_networks_cache = {}

# Data harga dan timestamp
binance_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}
kucoin_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}
bybit_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}
okx_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}
gate_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}
mexc_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}
htx_prices_data = {"prices": {}, "volumes": {}, "timestamp": None}

# Waktu kedaluwarsa cache dalam detik
CACHE_EXPIRY = 3600  # 1 jam untuk data jaringan dan status trading
PRICE_DATA_EXPIRY = 30  # 30 detik untuk data harga

# Jaringan yang umum didukung
COMMON_NETWORKS = [
    "ETH", "BSC", "BEP20", "ERC20", "TRC20", "TRON", "SOL", "SOLANA",
    "MATIC", "POLYGON", "ARBITRUM", "OPTIMISM", "AVAX", "AVALANCHE", "BTC"
]

# Data statis untuk jaringan yang didukung oleh koin populer
STATIC_NETWORK_DATA = {
    "USDT": {
        "binance": ["BEP20", "ERC20", "TRC20", "SOL", "MATIC", "ARBITRUM", "OPTIMISM", "AVAX"],
        "kucoin": ["ERC20", "TRC20", "SOL", "MATIC", "ARBITRUM", "OPTIMISM", "AVAX"],
        "bybit": ["ERC20", "TRC20", "SOL", "ARBITRUM", "OPTIMISM", "BEP20"],
        "okx": ["ERC20", "TRC20", "SOL", "ARBITRUM", "OPTIMISM", "BEP20"],
        "gate": ["ERC20", "TRC20", "SOL", "ARBITRUM", "OPTIMISM", "BEP20"],
        "mexc": ["ERC20", "TRC20", "SOL", "ARBITRUM", "OPTIMISM", "BEP20"],
        "htx": ["ERC20", "TRC20", "SOL", "ARBITRUM", "OPTIMISM", "BEP20"]
    },
    "BTC": {
        "binance": ["BTC", "BEP20", "ERC20", "TRC20", "SOL"],
        "kucoin": ["BTC", "ERC20", "TRC20", "SOL"],
        "bybit": ["BTC", "ERC20", "TRC20", "SOL"],
        "okx": ["BTC", "ERC20", "TRC20", "SOL"],
        "gate": ["BTC", "ERC20", "TRC20", "SOL"],
        "mexc": ["BTC", "ERC20", "TRC20", "SOL"],
        "htx": ["BTC", "ERC20", "TRC20", "SOL"]
    },
    "ETH": {
        "binance": ["ETH", "BEP20", "ARBITRUM", "OPTIMISM"],
        "kucoin": ["ETH", "ARBITRUM", "OPTIMISM"],
        "bybit": ["ETH", "ARBITRUM", "OPTIMISM", "BEP20"],
        "okx": ["ETH", "ARBITRUM", "OPTIMISM", "BEP20"],
        "gate": ["ETH", "ARBITRUM", "OPTIMISM", "BEP20"],
        "mexc": ["ETH", "ARBITRUM", "OPTIMISM", "BEP20"],
        "htx": ["ETH", "ARBITRUM", "OPTIMISM", "BEP20"]
    },
    "BNB": {
        "binance": ["BEP20", "BEP2"],
        "kucoin": ["BEP20"],
        "bybit": ["BEP20"],
        "okx": ["BEP20"],
        "gate": ["BEP20"],
        "mexc": ["BEP20"],
        "htx": ["BEP20"]
    },
    "SOL": {
        "binance": ["SOL", "BEP20"],
        "kucoin": ["SOL"],
        "bybit": ["SOL"],
        "okx": ["SOL"],
        "gate": ["SOL"],
        "mexc": ["SOL"],
        "htx": ["SOL"]
    },
    "MATIC": {
        "binance": ["MATIC", "BEP20", "ERC20"],
        "kucoin": ["MATIC", "ERC20"],
        "bybit": ["MATIC", "ERC20"],
        "okx": ["MATIC", "ERC20"],
        "gate": ["MATIC", "ERC20"],
        "mexc": ["MATIC", "ERC20"],
        "htx": ["MATIC", "ERC20"]
    },
    "AVAX": {
        "binance": ["AVAX", "BEP20", "ERC20"],
        "kucoin": ["AVAX", "ERC20"],
        "bybit": ["AVAX", "ERC20"],
        "okx": ["AVAX", "ERC20"],
        "gate": ["AVAX", "ERC20"],
        "mexc": ["AVAX", "ERC20"],
        "htx": ["AVAX", "ERC20"]
    }
}


def safe_float(value: Any, default: float = 0.0) -> float:
    """Konversi nilai ke float dengan aman"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def normalize_binance_symbol(symbol: str) -> str:
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


def normalize_kucoin_symbol(symbol: str) -> str:
    """Menormalisasi simbol KuCoin"""
    if "-" in symbol:
        base, quote = symbol.split("-")
        return f"{base}/{quote}"

    return symbol


def normalize_bybit_symbol(symbol: str) -> str:
    """Menormalisasi simbol ByBit"""
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}/USDT"
    elif symbol.endswith("BTC"):
        base = symbol[:-3]
        return f"{base}/BTC"
    elif symbol.endswith("ETH"):
        base = symbol[:-3]
        return f"{base}/ETH"
    elif symbol.endswith("USD"):
        base = symbol[:-3]
        return f"{base}/USD"

    # Jika format tidak dikenali, coba split dengan pemisah
    if "/" in symbol:
        return symbol

    return symbol


def normalize_okx_symbol(symbol: str) -> str:
    """Menormalisasi simbol OKX"""
    if "-" in symbol:
        base, quote = symbol.split("-")
        return f"{base}/{quote}"

    # Jika format tidak dikenali, coba split dengan pemisah
    if "/" in symbol:
        return symbol

    return symbol


def normalize_gate_symbol(symbol: str) -> str:
    """Menormalisasi simbol Gate.io"""
    if "_" in symbol:
        base, quote = symbol.split("_")
        return f"{base}/{quote}"

    # Jika format tidak dikenali, coba split dengan pemisah
    if "/" in symbol:
        return symbol

    return symbol


def normalize_mexc_symbol(symbol: str) -> str:
    """Menormalisasi simbol MEXC"""
    # MEXC menggunakan format yang sama dengan Binance (BTCUSDT)
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}/USDT"
    elif symbol.endswith("BTC"):
        base = symbol[:-3]
        return f"{base}/BTC"
    elif symbol.endswith("ETH"):
        base = symbol[:-3]
        return f"{base}/ETH"
    elif symbol.endswith("USD"):
        base = symbol[:-3]
        return f"{base}/USD"

    # Jika format tidak dikenali, coba split dengan pemisah
    if "/" in symbol:
        return symbol

    return symbol


def normalize_htx_symbol(symbol: str) -> str:
    """Menormalisasi simbol HTX (Huobi)"""
    # HTX menggunakan format seperti btcusdt (lowercase)
    symbol = symbol.upper()

    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}/USDT"
    elif symbol.endswith("BTC"):
        base = symbol[:-3]
        return f"{base}/BTC"
    elif symbol.endswith("ETH"):
        base = symbol[:-3]
        return f"{base}/ETH"
    elif symbol.endswith("USD"):
        base = symbol[:-3]
        return f"{base}/USD"

    # Jika format tidak dikenali, coba split dengan pemisah
    if "/" in symbol:
        return symbol

    return symbol


def get_binance_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol Binance sekaligus"""
    global binance_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in binance_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in binance_trading_status_cache:
            cache_time, is_tradable = binance_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari Binance
        response = requests.get(
            f"{BINANCE_API_URL}/exchangeInfo",
            timeout=10
        )
        data = response.json()

        # Buat dictionary untuk mempercepat pencarian
        symbol_status = {}
        if "symbols" in data:
            for symbol_info in data["symbols"]:
                symbol_name = symbol_info["symbol"]
                is_tradable = symbol_info["status"] == "TRADING"
                symbol_status[symbol_name] = is_tradable

                # Update cache
                binance_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

        # Periksa status trading untuk setiap simbol yang diminta
        for symbol in symbols_to_check:
            if symbol in symbol_status:
                result[symbol] = symbol_status[symbol]
            else:
                # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                result[symbol] = False
                binance_trading_status_cache[symbol] = (datetime.now(), False)

        return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading Binance: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_binance_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di Binance"""
    result = get_binance_trading_status([symbol])
    return result.get(symbol, True)


def get_binance_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk withdraw dari Binance"""
    global binance_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in binance_networks_cache:
        cache_time, networks = binance_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["binance"]
        binance_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # Jika tidak ada data statis, coba dapatkan dari API
    try:
        # Dapatkan informasi jaringan dari Binance
        response = requests.get(
            f"{BINANCE_API_URL}/capital/config/getall",
            timeout=10
        )
        data = response.json()

        # Cari coin dalam daftar
        networks = []
        for coin_info in data:
            if coin_info["coin"] == coin:
                # Dapatkan jaringan yang didukung
                for network_info in coin_info["networkList"]:
                    if network_info["withdrawEnable"]:
                        network_name = network_info["network"]
                        # Normalisasi nama jaringan
                        for common_network in COMMON_NETWORKS:
                            if common_network.lower() in network_name.lower():
                                networks.append(common_network)
                                break
                        else:
                            networks.append(network_name)

                # Simpan ke cache
                binance_networks_cache[coin] = (datetime.now(), networks)
                return networks

        # Jika coin tidak ditemukan, gunakan data default
        default_networks = ["BEP20", "ERC20"]
        binance_networks_cache[coin] = (datetime.now(), default_networks)
        return default_networks

    except Exception as e:
        logger.debug(f"Error mendapatkan jaringan Binance untuk {coin}: {e}")
        # Gunakan data default jika terjadi error
        default_networks = ["BEP20", "ERC20"]
        binance_networks_cache[coin] = (datetime.now(), default_networks)
        return default_networks


def get_binance_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari Binance"""
    global binance_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if binance_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - binance_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and binance_prices_data["prices"]:
        logger.debug(f"Menggunakan data Binance dari cache (umur: {data_age_seconds:.1f} detik)")
        return binance_prices_data["prices"], binance_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari Binance...")
        response = requests.get(f"{BINANCE_API_URL}/ticker/24hr", timeout=10)
        data = response.json()

        prices = {}
        volumes = {}
        for ticker in data:
            symbol = ticker["symbol"]
            price = safe_float(ticker["lastPrice"])
            volume = safe_float(ticker["quoteVolume"])  # Volume dalam mata uang quote

            prices[symbol] = price
            volumes[symbol] = volume

        # Update data global dengan timestamp
        binance_prices_data["prices"] = prices
        binance_prices_data["volumes"] = volumes
        binance_prices_data["timestamp"] = current_time

        logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari Binance")
        return prices, volumes

    except Exception as e:
        logger.error(f"Error mendapatkan harga Binance: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if binance_prices_data["prices"]:
            logger.warning(f"Menggunakan data Binance lama (umur: {data_age_seconds:.1f} detik)")
            return binance_prices_data["prices"], binance_prices_data["volumes"]
        return {}, {}


def get_kucoin_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol KuCoin sekaligus"""
    global kucoin_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in kucoin_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in kucoin_trading_status_cache:
            cache_time, is_tradable = kucoin_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari KuCoin
        response = requests.get(
            f"{KUCOIN_API_URL}/api/v1/symbols",
            timeout=10
        )
        data = response.json()

        if data["code"] == "200000" and "data" in data:
            # Buat dictionary untuk mempercepat pencarian
            symbol_status = {}
            for symbol_info in data["data"]:
                symbol_name = symbol_info["symbol"]
                is_tradable = symbol_info["enableTrading"]
                symbol_status[symbol_name] = is_tradable

                # Update cache
                kucoin_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

            # Periksa status trading untuk setiap simbol yang diminta
            for symbol in symbols_to_check:
                if symbol in symbol_status:
                    result[symbol] = symbol_status[symbol]
                else:
                    # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                    result[symbol] = False
                    kucoin_trading_status_cache[symbol] = (datetime.now(), False)

            return result
        else:
            logger.error(f"Gagal mendapatkan informasi simbol KuCoin: {data}")
            # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
            for symbol in symbols_to_check:
                result[symbol] = True
            return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading KuCoin: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_kucoin_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di KuCoin"""
    result = get_kucoin_trading_status([symbol])
    return result.get(symbol, True)


def get_bybit_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol ByBit sekaligus"""
    global bybit_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in bybit_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in bybit_trading_status_cache:
            cache_time, is_tradable = bybit_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari ByBit
        response = requests.get(
            f"{BYBIT_API_URL}/market/instruments-info",
            params={"category": "spot"},
            timeout=10
        )
        data = response.json()

        if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
            # Buat dictionary untuk mempercepat pencarian
            symbol_status = {}
            for symbol_info in data["result"]["list"]:
                symbol_name = symbol_info["symbol"]
                is_tradable = symbol_info["status"] == "Trading"
                symbol_status[symbol_name] = is_tradable

                # Update cache
                bybit_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

            # Periksa status trading untuk setiap simbol yang diminta
            for symbol in symbols_to_check:
                if symbol in symbol_status:
                    result[symbol] = symbol_status[symbol]
                else:
                    # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                    result[symbol] = False
                    bybit_trading_status_cache[symbol] = (datetime.now(), False)

            return result
        else:
            logger.error(f"Gagal mendapatkan informasi simbol ByBit: {data}")
            # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
            for symbol in symbols_to_check:
                result[symbol] = True
            return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading ByBit: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_bybit_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di ByBit"""
    result = get_bybit_trading_status([symbol])
    return result.get(symbol, True)


def get_okx_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol OKX sekaligus"""
    global okx_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in okx_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in okx_trading_status_cache:
            cache_time, is_tradable = okx_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari OKX
        response = requests.get(
            f"{OKX_API_URL}/public/instruments",
            params={"instType": "SPOT"},
            timeout=10
        )
        data = response.json()

        if data["code"] == "0" and "data" in data:
            # Buat dictionary untuk mempercepat pencarian
            symbol_status = {}
            for symbol_info in data["data"]:
                symbol_name = symbol_info["instId"]
                is_tradable = symbol_info["state"] == "live"
                symbol_status[symbol_name] = is_tradable

                # Update cache
                okx_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

            # Periksa status trading untuk setiap simbol yang diminta
            for symbol in symbols_to_check:
                if symbol in symbol_status:
                    result[symbol] = symbol_status[symbol]
                else:
                    # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                    result[symbol] = False
                    okx_trading_status_cache[symbol] = (datetime.now(), False)

            return result
        else:
            logger.error(f"Gagal mendapatkan informasi simbol OKX: {data}")
            # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
            for symbol in symbols_to_check:
                result[symbol] = True
            return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading OKX: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_okx_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di OKX"""
    result = get_okx_trading_status([symbol])
    return result.get(symbol, True)


def get_gate_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol Gate.io sekaligus"""
    global gate_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in gate_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in gate_trading_status_cache:
            cache_time, is_tradable = gate_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari Gate.io
        response = requests.get(
            f"{GATE_API_URL}/spot/currency_pairs",
            timeout=10
        )
        data = response.json()

        if isinstance(data, list):
            # Buat dictionary untuk mempercepat pencarian
            symbol_status = {}
            for symbol_info in data:
                symbol_name = symbol_info["id"]  # Format: BTC_USDT
                is_tradable = symbol_info["trade_status"] == "tradable"
                symbol_status[symbol_name] = is_tradable

                # Update cache
                gate_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

            # Periksa status trading untuk setiap simbol yang diminta
            for symbol in symbols_to_check:
                if symbol in symbol_status:
                    result[symbol] = symbol_status[symbol]
                else:
                    # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                    result[symbol] = False
                    gate_trading_status_cache[symbol] = (datetime.now(), False)

            return result
        else:
            logger.error(f"Gagal mendapatkan informasi simbol Gate.io: {data}")
            # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
            for symbol in symbols_to_check:
                result[symbol] = True
            return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading Gate.io: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_gate_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di Gate.io"""
    result = get_gate_trading_status([symbol])
    return result.get(symbol, True)


def get_mexc_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol MEXC sekaligus"""
    global mexc_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in mexc_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in mexc_trading_status_cache:
            cache_time, is_tradable = mexc_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari MEXC
        response = requests.get(
            f"{MEXC_API_URL}/exchangeInfo",
            timeout=10
        )
        data = response.json()

        if "symbols" in data:
            # Buat dictionary untuk mempercepat pencarian
            symbol_status = {}
            for symbol_info in data["symbols"]:
                symbol_name = symbol_info["symbol"]  # Format: BTCUSDT
                is_tradable = symbol_info["status"] == "ENABLED"
                symbol_status[symbol_name] = is_tradable

                # Update cache
                mexc_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

            # Periksa status trading untuk setiap simbol yang diminta
            for symbol in symbols_to_check:
                if symbol in symbol_status:
                    result[symbol] = symbol_status[symbol]
                else:
                    # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                    result[symbol] = False
                    mexc_trading_status_cache[symbol] = (datetime.now(), False)

            return result
        else:
            logger.error(f"Gagal mendapatkan informasi simbol MEXC: {data}")
            # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
            for symbol in symbols_to_check:
                result[symbol] = True
            return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading MEXC: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_mexc_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di MEXC"""
    result = get_mexc_trading_status([symbol])
    return result.get(symbol, True)


def get_htx_trading_status(symbols: List[str] = None) -> Dict[str, bool]:
    """Mendapatkan status trading untuk beberapa simbol HTX (Huobi) sekaligus"""
    global htx_trading_status_cache
    result = {}
    symbols_to_check = []

    # Jika tidak ada simbol yang diberikan, kembalikan cache
    if not symbols:
        return {s: status for s, (_, status) in htx_trading_status_cache.items()}

    # Cek cache terlebih dahulu
    for symbol in symbols:
        if symbol in htx_trading_status_cache:
            cache_time, is_tradable = htx_trading_status_cache[symbol]
            # Jika cache belum kedaluwarsa, gunakan nilai dari cache
            if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
                result[symbol] = is_tradable
            else:
                symbols_to_check.append(symbol)
        else:
            symbols_to_check.append(symbol)

    # Jika semua simbol sudah ada di cache, kembalikan hasil
    if not symbols_to_check:
        return result

    try:
        # Dapatkan informasi semua simbol dari HTX (Huobi)
        response = requests.get(
            "https://api.huobi.pro/v1/common/symbols",
            timeout=10
        )
        data = response.json()

        if data["status"] == "ok" and "data" in data:
            # Buat dictionary untuk mempercepat pencarian
            symbol_status = {}
            for symbol_info in data["data"]:
                symbol_name = symbol_info["symbol"]  # Format: btcusdt
                is_tradable = symbol_info["state"] == "online"
                symbol_status[symbol_name] = is_tradable

                # Update cache
                htx_trading_status_cache[symbol_name] = (datetime.now(), is_tradable)

            # Periksa status trading untuk setiap simbol yang diminta
            for symbol in symbols_to_check:
                # HTX menggunakan simbol lowercase
                symbol_lower = symbol.lower()
                if symbol_lower in symbol_status:
                    result[symbol] = symbol_status[symbol_lower]
                else:
                    # Jika simbol tidak ditemukan, anggap tidak dapat diperdagangkan
                    result[symbol] = False
                    htx_trading_status_cache[symbol] = (datetime.now(), False)

            return result
        else:
            logger.error(f"Gagal mendapatkan informasi simbol HTX: {data}")
            # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
            for symbol in symbols_to_check:
                result[symbol] = True
            return result

    except Exception as e:
        logger.error(f"Error mendapatkan status trading HTX: {e}")
        # Jika terjadi error, anggap semua simbol masih dapat diperdagangkan
        for symbol in symbols_to_check:
            result[symbol] = True
        return result


def is_htx_symbol_tradable(symbol: str) -> bool:
    """Memeriksa apakah simbol masih dapat diperdagangkan di HTX (Huobi)"""
    result = get_htx_trading_status([symbol])
    return result.get(symbol, True)


def get_kucoin_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk deposit ke KuCoin"""
    global kucoin_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in kucoin_networks_cache:
        cache_time, networks = kucoin_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["kucoin"]
        kucoin_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # Jika tidak ada data statis, coba dapatkan dari API
    try:
        # Dapatkan informasi jaringan dari KuCoin
        response = requests.get(
            f"{KUCOIN_API_URL}/api/v1/currencies/{coin}",
            timeout=10
        )
        data = response.json()

        if data["code"] == "200000" and "data" in data:
            # Dapatkan jaringan yang didukung
            networks = []
            for chain_info in data["data"]["chains"]:
                if chain_info["isDepositEnabled"]:
                    chain_name = chain_info["chainName"]
                    # Normalisasi nama jaringan
                    for common_network in COMMON_NETWORKS:
                        if common_network.lower() in chain_name.lower():
                            networks.append(common_network)
                            break
                    else:
                        networks.append(chain_name)

            # Simpan ke cache
            kucoin_networks_cache[coin] = (datetime.now(), networks)
            return networks
        else:
            logger.debug(f"Gagal mendapatkan informasi jaringan KuCoin untuk {coin}: {data}")
            # Gunakan data default jika gagal
            default_networks = ["ERC20", "TRC20"]
            kucoin_networks_cache[coin] = (datetime.now(), default_networks)
            return default_networks

    except Exception as e:
        logger.debug(f"Error mendapatkan jaringan KuCoin untuk {coin}: {e}")
        # Gunakan data default jika terjadi error
        default_networks = ["ERC20", "TRC20"]
        kucoin_networks_cache[coin] = (datetime.now(), default_networks)
        return default_networks


def get_bybit_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk deposit ke ByBit"""
    global bybit_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in bybit_networks_cache:
        cache_time, networks = bybit_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["bybit"]
        bybit_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # Jika tidak ada data statis, gunakan data default
    # ByBit tidak memiliki API publik untuk mendapatkan informasi jaringan
    # Jadi kita gunakan data default
    default_networks = ["ERC20", "TRC20"]
    bybit_networks_cache[coin] = (datetime.now(), default_networks)
    return default_networks


def get_okx_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk deposit ke OKX"""
    global okx_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in okx_networks_cache:
        cache_time, networks = okx_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["okx"]
        okx_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # Jika tidak ada data statis, coba dapatkan dari API
    try:
        # Dapatkan informasi jaringan dari OKX
        response = requests.get(
            f"{OKX_API_URL}/asset/deposit-address",
            params={"ccy": coin},
            timeout=10
        )
        data = response.json()

        if data["code"] == "0" and "data" in data:
            # Dapatkan jaringan yang didukung
            networks = []
            for chain_info in data["data"]:
                if chain_info["chain"]:
                    chain_name = chain_info["chain"]
                    # Normalisasi nama jaringan
                    for common_network in COMMON_NETWORKS:
                        if common_network.lower() in chain_name.lower():
                            networks.append(common_network)
                            break
                    else:
                        networks.append(chain_name)

            # Simpan ke cache
            okx_networks_cache[coin] = (datetime.now(), networks)
            return networks
        else:
            logger.debug(f"Gagal mendapatkan informasi jaringan OKX untuk {coin}: {data}")
            # Gunakan data default jika gagal
            default_networks = ["ERC20", "TRC20"]
            okx_networks_cache[coin] = (datetime.now(), default_networks)
            return default_networks

    except Exception as e:
        logger.debug(f"Error mendapatkan jaringan OKX untuk {coin}: {e}")
        # Gunakan data default jika terjadi error
        default_networks = ["ERC20", "TRC20"]
        okx_networks_cache[coin] = (datetime.now(), default_networks)
        return default_networks


def get_gate_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk deposit ke Gate.io"""
    global gate_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in gate_networks_cache:
        cache_time, networks = gate_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["gate"]
        gate_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # Jika tidak ada data statis, coba dapatkan dari API
    try:
        # Dapatkan informasi jaringan dari Gate.io
        response = requests.get(
            f"{GATE_API_URL}/wallet/currency_chains",
            params={"currency": coin},
            timeout=10
        )
        data = response.json()

        if isinstance(data, list):
            # Dapatkan jaringan yang didukung
            networks = []
            for chain_info in data:
                if chain_info["chain"]:
                    chain_name = chain_info["chain"]
                    # Normalisasi nama jaringan
                    for common_network in COMMON_NETWORKS:
                        if common_network.lower() in chain_name.lower():
                            networks.append(common_network)
                            break
                    else:
                        networks.append(chain_name)

            # Simpan ke cache
            gate_networks_cache[coin] = (datetime.now(), networks)
            return networks
        else:
            logger.debug(f"Gagal mendapatkan informasi jaringan Gate.io untuk {coin}: {data}")
            # Gunakan data default jika gagal
            default_networks = ["ERC20", "TRC20"]
            gate_networks_cache[coin] = (datetime.now(), default_networks)
            return default_networks

    except Exception as e:
        logger.debug(f"Error mendapatkan jaringan Gate.io untuk {coin}: {e}")
        # Gunakan data default jika terjadi error
        default_networks = ["ERC20", "TRC20"]
        gate_networks_cache[coin] = (datetime.now(), default_networks)
        return default_networks


def get_mexc_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk deposit ke MEXC"""
    global mexc_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in mexc_networks_cache:
        cache_time, networks = mexc_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["mexc"]
        mexc_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # MEXC tidak memiliki API publik untuk mendapatkan informasi jaringan
    # Jadi kita gunakan data default
    default_networks = ["ERC20", "TRC20"]
    mexc_networks_cache[coin] = (datetime.now(), default_networks)
    return default_networks


def get_htx_networks(coin: str) -> List[str]:
    """Mendapatkan jaringan yang didukung untuk deposit ke HTX (Huobi)"""
    global htx_networks_cache

    # Normalisasi nama coin
    coin = coin.upper()

    # Cek cache terlebih dahulu
    if coin in htx_networks_cache:
        cache_time, networks = htx_networks_cache[coin]
        # Jika cache belum kedaluwarsa, gunakan nilai dari cache
        if (datetime.now() - cache_time).total_seconds() < CACHE_EXPIRY:
            return networks

    # Cek data statis
    if coin in STATIC_NETWORK_DATA:
        networks = STATIC_NETWORK_DATA[coin]["htx"]
        htx_networks_cache[coin] = (datetime.now(), networks)
        return networks

    # Jika tidak ada data statis, coba dapatkan dari API
    try:
        # Dapatkan informasi jaringan dari HTX (Huobi)
        response = requests.get(
            "https://api.huobi.pro/v2/reference/currencies",
            params={"currency": coin},
            timeout=10
        )
        data = response.json()

        if data["code"] == 200 and "data" in data:
            # Dapatkan jaringan yang didukung
            networks = []
            for coin_info in data["data"]:
                if coin_info["currency"].upper() == coin.upper():
                    for chain_info in coin_info["chains"]:
                        if chain_info["depositStatus"] == "allowed":
                            chain_name = chain_info["chain"]
                            # Normalisasi nama jaringan
                            for common_network in COMMON_NETWORKS:
                                if common_network.lower() in chain_name.lower():
                                    networks.append(common_network)
                                    break
                            else:
                                networks.append(chain_name)

            # Simpan ke cache
            htx_networks_cache[coin] = (datetime.now(), networks)
            return networks
        else:
            logger.debug(f"Gagal mendapatkan informasi jaringan HTX untuk {coin}: {data}")
            # Gunakan data default jika gagal
            default_networks = ["ERC20", "TRC20"]
            htx_networks_cache[coin] = (datetime.now(), default_networks)
            return default_networks

    except Exception as e:
        logger.debug(f"Error mendapatkan jaringan HTX untuk {coin}: {e}")
        # Gunakan data default jika terjadi error
        default_networks = ["ERC20", "TRC20"]
        htx_networks_cache[coin] = (datetime.now(), default_networks)
        return default_networks


def get_kucoin_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari KuCoin"""
    global kucoin_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if kucoin_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - kucoin_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and kucoin_prices_data["prices"]:
        logger.debug(f"Menggunakan data KuCoin dari cache (umur: {data_age_seconds:.1f} detik)")
        return kucoin_prices_data["prices"], kucoin_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari KuCoin...")
        response = requests.get(f"{KUCOIN_API_URL}/api/v1/market/allTickers", timeout=10)
        data = response.json()

        if data["code"] == "200000":
            prices = {}
            volumes = {}
            for ticker in data["data"]["ticker"]:
                symbol = ticker["symbol"]
                price = safe_float(ticker["last"])
                volume = safe_float(ticker["volValue"])  # Volume dalam USD

                prices[symbol] = price
                volumes[symbol] = volume

            # Update data global dengan timestamp
            kucoin_prices_data["prices"] = prices
            kucoin_prices_data["volumes"] = volumes
            kucoin_prices_data["timestamp"] = current_time

            logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari KuCoin")
            return prices, volumes
        else:
            logger.error(f"Gagal mengambil harga KuCoin: {data}")
            # Jika gagal dan ada data lama, gunakan data lama
            if kucoin_prices_data["prices"]:
                logger.warning(f"Menggunakan data KuCoin lama (umur: {data_age_seconds:.1f} detik)")
                return kucoin_prices_data["prices"], kucoin_prices_data["volumes"]
            return {}, {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga KuCoin: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if kucoin_prices_data["prices"]:
            logger.warning(f"Menggunakan data KuCoin lama (umur: {data_age_seconds:.1f} detik)")
            return kucoin_prices_data["prices"], kucoin_prices_data["volumes"]
        return {}, {}


def get_bybit_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari ByBit"""
    global bybit_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if bybit_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - bybit_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and bybit_prices_data["prices"]:
        logger.debug(f"Menggunakan data ByBit dari cache (umur: {data_age_seconds:.1f} detik)")
        return bybit_prices_data["prices"], bybit_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari ByBit...")
        response = requests.get(f"{BYBIT_API_URL}/market/tickers", params={"category": "spot"}, timeout=10)
        data = response.json()

        if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
            prices = {}
            volumes = {}
            for ticker in data["result"]["list"]:
                symbol = ticker["symbol"]
                price = safe_float(ticker["lastPrice"])
                volume = safe_float(ticker["volume24h"]) * price  # Konversi volume ke USD

                prices[symbol] = price
                volumes[symbol] = volume

            # Update data global dengan timestamp
            bybit_prices_data["prices"] = prices
            bybit_prices_data["volumes"] = volumes
            bybit_prices_data["timestamp"] = current_time

            logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari ByBit")
            return prices, volumes
        else:
            logger.error(f"Gagal mengambil harga ByBit: {data}")
            # Jika gagal dan ada data lama, gunakan data lama
            if bybit_prices_data["prices"]:
                logger.warning(f"Menggunakan data ByBit lama (umur: {data_age_seconds:.1f} detik)")
                return bybit_prices_data["prices"], bybit_prices_data["volumes"]
            return {}, {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga ByBit: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if bybit_prices_data["prices"]:
            logger.warning(f"Menggunakan data ByBit lama (umur: {data_age_seconds:.1f} detik)")
            return bybit_prices_data["prices"], bybit_prices_data["volumes"]
        return {}, {}


def get_okx_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari OKX"""
    global okx_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if okx_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - okx_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and okx_prices_data["prices"]:
        logger.debug(f"Menggunakan data OKX dari cache (umur: {data_age_seconds:.1f} detik)")
        return okx_prices_data["prices"], okx_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari OKX...")
        response = requests.get(f"{OKX_API_URL}/market/tickers", params={"instType": "SPOT"}, timeout=10)
        data = response.json()

        if data["code"] == "0" and "data" in data:
            prices = {}
            volumes = {}
            for ticker in data["data"]:
                symbol = ticker["instId"]  # Format: BTC-USDT
                price = safe_float(ticker["last"])
                # OKX API menggunakan vol24h untuk volume dalam base currency dan volCcy24h untuk volume dalam quote currency
                volume = safe_float(ticker.get("vol24h", 0)) * price  # Konversi volume ke USD

                prices[symbol] = price
                volumes[symbol] = volume

            # Update data global dengan timestamp
            okx_prices_data["prices"] = prices
            okx_prices_data["volumes"] = volumes
            okx_prices_data["timestamp"] = current_time

            logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari OKX")
            return prices, volumes
        else:
            logger.error(f"Gagal mengambil harga OKX: {data}")
            # Jika gagal dan ada data lama, gunakan data lama
            if okx_prices_data["prices"]:
                logger.warning(f"Menggunakan data OKX lama (umur: {data_age_seconds:.1f} detik)")
                return okx_prices_data["prices"], okx_prices_data["volumes"]
            return {}, {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga OKX: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if okx_prices_data["prices"]:
            logger.warning(f"Menggunakan data OKX lama (umur: {data_age_seconds:.1f} detik)")
            return okx_prices_data["prices"], okx_prices_data["volumes"]
        return {}, {}


def get_gate_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari Gate.io"""
    global gate_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if gate_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - gate_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and gate_prices_data["prices"]:
        logger.debug(f"Menggunakan data Gate.io dari cache (umur: {data_age_seconds:.1f} detik)")
        return gate_prices_data["prices"], gate_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari Gate.io...")
        response = requests.get(f"{GATE_API_URL}/spot/tickers", timeout=10)
        data = response.json()

        if isinstance(data, list):
            prices = {}
            volumes = {}
            for ticker in data:
                symbol = ticker["currency_pair"]  # Format: BTC_USDT
                price = safe_float(ticker["last"])
                volume = safe_float(ticker["quote_volume"])  # Volume dalam quote currency (USDT)

                prices[symbol] = price
                volumes[symbol] = volume

            # Update data global dengan timestamp
            gate_prices_data["prices"] = prices
            gate_prices_data["volumes"] = volumes
            gate_prices_data["timestamp"] = current_time

            logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari Gate.io")
            return prices, volumes
        else:
            logger.error(f"Gagal mengambil harga Gate.io: {data}")
            # Jika gagal dan ada data lama, gunakan data lama
            if gate_prices_data["prices"]:
                logger.warning(f"Menggunakan data Gate.io lama (umur: {data_age_seconds:.1f} detik)")
                return gate_prices_data["prices"], gate_prices_data["volumes"]
            return {}, {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga Gate.io: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if gate_prices_data["prices"]:
            logger.warning(f"Menggunakan data Gate.io lama (umur: {data_age_seconds:.1f} detik)")
            return gate_prices_data["prices"], gate_prices_data["volumes"]
        return {}, {}


def get_mexc_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari MEXC"""
    global mexc_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if mexc_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - mexc_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and mexc_prices_data["prices"]:
        logger.debug(f"Menggunakan data MEXC dari cache (umur: {data_age_seconds:.1f} detik)")
        return mexc_prices_data["prices"], mexc_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari MEXC...")
        response = requests.get(f"{MEXC_API_URL}/ticker/24hr", timeout=10)
        data = response.json()

        if isinstance(data, list):
            prices = {}
            volumes = {}
            for ticker in data:
                symbol = ticker["symbol"]  # Format: BTCUSDT
                price = safe_float(ticker["lastPrice"])
                volume = safe_float(ticker["quoteVolume"])  # Volume dalam quote currency (USDT)

                prices[symbol] = price
                volumes[symbol] = volume

            # Update data global dengan timestamp
            mexc_prices_data["prices"] = prices
            mexc_prices_data["volumes"] = volumes
            mexc_prices_data["timestamp"] = current_time

            logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari MEXC")
            return prices, volumes
        else:
            logger.error(f"Gagal mengambil harga MEXC: {data}")
            # Jika gagal dan ada data lama, gunakan data lama
            if mexc_prices_data["prices"]:
                logger.warning(f"Menggunakan data MEXC lama (umur: {data_age_seconds:.1f} detik)")
                return mexc_prices_data["prices"], mexc_prices_data["volumes"]
            return {}, {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga MEXC: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if mexc_prices_data["prices"]:
            logger.warning(f"Menggunakan data MEXC lama (umur: {data_age_seconds:.1f} detik)")
            return mexc_prices_data["prices"], mexc_prices_data["volumes"]
        return {}, {}


def get_htx_prices(force_refresh: bool = False) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Mendapatkan harga dan volume dari HTX (Huobi)"""
    global htx_prices_data

    # Cek apakah data masih valid atau perlu refresh
    current_time = datetime.now()
    data_age_seconds = float('inf')  # Default ke nilai tak terhingga

    if htx_prices_data["timestamp"] is not None:
        data_age_seconds = (current_time - htx_prices_data["timestamp"]).total_seconds()

    # Jika data masih valid dan tidak dipaksa refresh, gunakan data yang ada
    if not force_refresh and data_age_seconds < PRICE_DATA_EXPIRY and htx_prices_data["prices"]:
        logger.debug(f"Menggunakan data HTX dari cache (umur: {data_age_seconds:.1f} detik)")
        return htx_prices_data["prices"], htx_prices_data["volumes"]

    # Jika data sudah kedaluwarsa atau dipaksa refresh, ambil data baru
    try:
        logger.info("Mengambil data harga terbaru dari HTX...")
        response = requests.get(f"{HTX_API_URL}/tickers", timeout=10)
        data = response.json()

        if data["status"] == "ok" and "data" in data:
            prices = {}
            volumes = {}
            for ticker in data["data"]:
                symbol = ticker["symbol"]  # Format: btcusdt
                price = safe_float(ticker["close"])
                volume = safe_float(ticker["vol"]) * price  # Konversi volume ke USD

                prices[symbol] = price
                volumes[symbol] = volume

            # Update data global dengan timestamp
            htx_prices_data["prices"] = prices
            htx_prices_data["volumes"] = volumes
            htx_prices_data["timestamp"] = current_time

            logger.info(f"Berhasil mengambil {len(prices)} harga terbaru dari HTX")
            return prices, volumes
        else:
            logger.error(f"Gagal mengambil harga HTX: {data}")
            # Jika gagal dan ada data lama, gunakan data lama
            if htx_prices_data["prices"]:
                logger.warning(f"Menggunakan data HTX lama (umur: {data_age_seconds:.1f} detik)")
                return htx_prices_data["prices"], htx_prices_data["volumes"]
            return {}, {}

    except Exception as e:
        logger.error(f"Error mendapatkan harga HTX: {e}")
        # Jika gagal dan ada data lama, gunakan data lama
        if htx_prices_data["prices"]:
            logger.warning(f"Menggunakan data HTX lama (umur: {data_age_seconds:.1f} detik)")
            return htx_prices_data["prices"], htx_prices_data["volumes"]
        return {}, {}


def find_common_pairs(
    binance_prices: Dict[str, float],
    kucoin_prices: Dict[str, float],
    bybit_prices: Dict[str, float],
    okx_prices: Dict[str, float],
    gate_prices: Dict[str, float],
    mexc_prices: Dict[str, float],
    htx_prices: Dict[str, float]
) -> Dict[str, Dict[str, str]]:
    """Menemukan pasangan trading yang sama di minimal 2 bursa"""
    normalized_binance = {normalize_binance_symbol(s): s for s in binance_prices.keys()}
    normalized_kucoin = {normalize_kucoin_symbol(s): s for s in kucoin_prices.keys()}
    normalized_bybit = {normalize_bybit_symbol(s): s for s in bybit_prices.keys()}
    normalized_okx = {normalize_okx_symbol(s): s for s in okx_prices.keys()}
    normalized_gate = {normalize_gate_symbol(s): s for s in gate_prices.keys()}
    normalized_mexc = {normalize_mexc_symbol(s): s for s in mexc_prices.keys()}
    normalized_htx = {normalize_htx_symbol(s): s for s in htx_prices.keys()}

    # Temukan pasangan yang sama di minimal 2 bursa
    common_pairs = {}

    # Buat set untuk semua simbol yang dinormalisasi
    all_normalized = set(normalized_binance.keys()) | set(normalized_kucoin.keys()) | \
                     set(normalized_bybit.keys()) | set(normalized_okx.keys()) | \
                     set(normalized_gate.keys()) | set(normalized_mexc.keys()) | \
                     set(normalized_htx.keys())

    # Untuk setiap simbol yang dinormalisasi, cek di bursa mana saja simbol tersebut ada
    for norm in all_normalized:
        exchanges = {}

        if norm in normalized_binance:
            exchanges["binance"] = normalized_binance[norm]

        if norm in normalized_kucoin:
            exchanges["kucoin"] = normalized_kucoin[norm]

        if norm in normalized_bybit:
            exchanges["bybit"] = normalized_bybit[norm]

        if norm in normalized_okx:
            exchanges["okx"] = normalized_okx[norm]

        if norm in normalized_gate:
            exchanges["gate"] = normalized_gate[norm]

        if norm in normalized_mexc:
            exchanges["mexc"] = normalized_mexc[norm]

        if norm in normalized_htx:
            exchanges["htx"] = normalized_htx[norm]

        # Jika simbol ada di minimal 2 bursa, tambahkan ke common_pairs
        if len(exchanges) >= 2:
            common_pairs[norm] = exchanges

    logger.info(f"Ditemukan {len(common_pairs)} pasangan trading yang sama di minimal 2 bursa")
    return common_pairs


def find_common_networks(coin: str) -> Dict[str, List[str]]:
    """Menemukan jaringan yang sama antara semua bursa untuk suatu coin"""
    binance_networks = get_binance_networks(coin)
    kucoin_networks = get_kucoin_networks(coin)
    bybit_networks = get_bybit_networks(coin)
    okx_networks = get_okx_networks(coin)
    gate_networks = get_gate_networks(coin)
    mexc_networks = get_mexc_networks(coin)
    htx_networks = get_htx_networks(coin)

    # Normalisasi nama jaringan
    normalized_binance = [network.upper() for network in binance_networks]
    normalized_kucoin = [network.upper() for network in kucoin_networks]
    normalized_bybit = [network.upper() for network in bybit_networks]
    normalized_okx = [network.upper() for network in okx_networks]
    normalized_gate = [network.upper() for network in gate_networks]
    normalized_mexc = [network.upper() for network in mexc_networks]
    normalized_htx = [network.upper() for network in htx_networks]

    # Buat dictionary untuk mempermudah pengecekan
    exchange_networks = {
        "binance": normalized_binance,
        "kucoin": normalized_kucoin,
        "bybit": normalized_bybit,
        "okx": normalized_okx,
        "gate": normalized_gate,
        "mexc": normalized_mexc,
        "htx": normalized_htx
    }

    # Buat set dari semua jaringan yang ada
    all_networks = set()
    for networks in exchange_networks.values():
        all_networks.update(networks)

    # Hitung berapa banyak bursa yang mendukung setiap jaringan
    network_support_count = {}
    for network in all_networks:
        count = sum(1 for exchange_net in exchange_networks.values() if network in exchange_net)
        network_support_count[network] = count

    # Urutkan jaringan berdasarkan jumlah bursa yang mendukung (dari yang terbanyak)
    sorted_networks = sorted(network_support_count.items(), key=lambda x: x[1], reverse=True)

    # Ambil jaringan yang didukung oleh minimal 2 bursa
    common_networks = [network for network, count in sorted_networks if count >= 2]

    # Jika tidak ada jaringan yang didukung oleh minimal 2 bursa, gunakan semua jaringan yang tersedia
    if not common_networks:
        result = {
            "binance": binance_networks,
            "kucoin": kucoin_networks,
            "bybit": bybit_networks,
            "okx": okx_networks,
            "gate": gate_networks,
            "mexc": mexc_networks,
            "htx": htx_networks,
            "common": []
        }
    else:
        result = {
            "binance": binance_networks,
            "kucoin": kucoin_networks,
            "bybit": bybit_networks,
            "okx": okx_networks,
            "gate": gate_networks,
            "mexc": mexc_networks,
            "htx": htx_networks,
            "common": common_networks
        }

    return result


def get_withdrawal_fee(coin: str, network: str, price: float = 0.0) -> float:
    """Mendapatkan biaya penarikan dalam USD"""
    # Normalisasi nama coin dan network
    coin = coin.upper()
    network = network.upper()

    # Cek apakah ada biaya spesifik untuk coin dan network
    if coin in WITHDRAWAL_FEES and network in WITHDRAWAL_FEES[coin]:
        fee = WITHDRAWAL_FEES[coin][network]
        # Jika fee negatif, berarti dalam token, konversi ke USD
        if fee < 0:
            return abs(fee) * price
        # Jika fee positif, berarti dalam USD
        return fee

    # Cek apakah ada biaya default untuk network
    if "default" in WITHDRAWAL_FEES and network in WITHDRAWAL_FEES["default"]:
        return WITHDRAWAL_FEES["default"][network]

    # Gunakan biaya default
    if "default" in WITHDRAWAL_FEES and "default" in WITHDRAWAL_FEES["default"]:
        return WITHDRAWAL_FEES["default"]["default"]

    # Jika tidak ada biaya yang ditemukan, gunakan nilai default
    return 5.0  # $5 USD


def get_gas_fee(network: str) -> float:
    """Mendapatkan biaya gas dalam USD"""
    # Normalisasi nama network
    network = network.upper()

    # Cek apakah ada biaya spesifik untuk network
    if network in GAS_FEES:
        return GAS_FEES[network]

    # Gunakan biaya default
    return GAS_FEES.get("default", 1.0)  # $1 USD


def get_trading_fee(exchange: str, order_type: str = "taker") -> float:
    """Mendapatkan biaya trading dalam persentase"""
    # Normalisasi nama exchange dan order_type
    exchange = exchange.lower()
    order_type = order_type.lower()

    # Cek apakah ada biaya spesifik untuk exchange dan order_type
    if exchange in TRADING_FEES and order_type in TRADING_FEES[exchange]:
        return TRADING_FEES[exchange][order_type]

    # Cek apakah ada biaya default untuk exchange
    if exchange in TRADING_FEES and "default" in TRADING_FEES[exchange]:
        return TRADING_FEES[exchange]["default"]

    # Gunakan biaya default
    return 0.1  # 0.1%


def find_best_network(coin: str, binance_price: float, common_networks: Dict[str, List[str]]) -> Dict[str, str]:
    """Menemukan jaringan terbaik (biaya terendah) untuk transfer"""
    best_network = None
    lowest_total_fee = float('inf')

    # Jika ada jaringan yang sama, pilih yang biayanya paling rendah
    if common_networks["common"]:
        for network in common_networks["common"]:
            # Hitung biaya penarikan
            withdrawal_fee = get_withdrawal_fee(coin, network, binance_price)
            # Hitung biaya gas
            gas_fee = get_gas_fee(network)
            # Total biaya
            total_fee = withdrawal_fee + gas_fee

            # Jika biaya lebih rendah, update jaringan terbaik
            if total_fee < lowest_total_fee:
                lowest_total_fee = total_fee
                best_network = network
    # Jika tidak ada jaringan yang sama, gunakan jaringan default
    else:
        best_network = "ERC20"  # Default ke ERC20
        withdrawal_fee = get_withdrawal_fee(coin, best_network, binance_price)
        gas_fee = get_gas_fee(best_network)
        lowest_total_fee = withdrawal_fee + gas_fee

    return {
        "network": best_network,
        "withdrawal_fee": get_withdrawal_fee(coin, best_network, binance_price),
        "gas_fee": get_gas_fee(best_network),
        "total_fee": lowest_total_fee
    }


def calculate_slippage(price: float, volume: float, side: str, slippage_factor: float = 0.001) -> float:
    """Menghitung estimasi slippage berdasarkan volume"""
    # Semakin tinggi volume, semakin rendah slippage
    volume_factor = min(1.0, 100000 / max(volume, 1))

    # Slippage lebih tinggi untuk pembelian
    if side == "buy":
        return price * (1 + slippage_factor * volume_factor)
    else:
        return price * (1 - slippage_factor * volume_factor)


def calculate_arbitrage(
    common_pairs: Dict[str, Dict[str, str]],
    binance_prices: Dict[str, float],
    binance_volumes: Dict[str, float],
    kucoin_prices: Dict[str, float],
    kucoin_volumes: Dict[str, float],
    bybit_prices: Dict[str, float],
    bybit_volumes: Dict[str, float],
    okx_prices: Dict[str, float],
    okx_volumes: Dict[str, float],
    gate_prices: Dict[str, float],
    gate_volumes: Dict[str, float],
    mexc_prices: Dict[str, float],
    mexc_volumes: Dict[str, float],
    htx_prices: Dict[str, float],
    htx_volumes: Dict[str, float]
) -> List[Dict]:
    """Menghitung peluang arbitrase"""
    opportunities = []
    checked_pairs = 0
    potential_pairs = 0

    # Kumpulkan semua simbol dari ketujuh bursa yang perlu diperiksa
    binance_symbols = []
    kucoin_symbols = []
    bybit_symbols = []
    okx_symbols = []
    gate_symbols = []
    mexc_symbols = []
    htx_symbols = []

    for exchange_pairs in common_pairs.values():
        if exchange_pairs.get("binance"):
            binance_symbols.append(exchange_pairs["binance"])
        if exchange_pairs.get("kucoin"):
            kucoin_symbols.append(exchange_pairs["kucoin"])
        if exchange_pairs.get("bybit"):
            bybit_symbols.append(exchange_pairs["bybit"])
        if exchange_pairs.get("okx"):
            okx_symbols.append(exchange_pairs["okx"])
        if exchange_pairs.get("gate"):
            gate_symbols.append(exchange_pairs["gate"])
        if exchange_pairs.get("mexc"):
            mexc_symbols.append(exchange_pairs["mexc"])
        if exchange_pairs.get("htx"):
            htx_symbols.append(exchange_pairs["htx"])

    # Dapatkan status trading untuk semua simbol sekaligus
    logger.info(f"Memeriksa status trading untuk {len(binance_symbols)} simbol Binance...")
    binance_trading_status = get_binance_trading_status(binance_symbols)

    logger.info(f"Memeriksa status trading untuk {len(kucoin_symbols)} simbol KuCoin...")
    kucoin_trading_status = get_kucoin_trading_status(kucoin_symbols)

    logger.info(f"Memeriksa status trading untuk {len(bybit_symbols)} simbol ByBit...")
    bybit_trading_status = get_bybit_trading_status(bybit_symbols)

    logger.info(f"Memeriksa status trading untuk {len(okx_symbols)} simbol OKX...")
    okx_trading_status = get_okx_trading_status(okx_symbols)

    logger.info(f"Memeriksa status trading untuk {len(gate_symbols)} simbol Gate.io...")
    gate_trading_status = get_gate_trading_status(gate_symbols)

    logger.info(f"Memeriksa status trading untuk {len(mexc_symbols)} simbol MEXC...")
    mexc_trading_status = get_mexc_trading_status(mexc_symbols)

    logger.info(f"Memeriksa status trading untuk {len(htx_symbols)} simbol HTX...")
    htx_trading_status = get_htx_trading_status(htx_symbols)

    for norm_pair, exchange_pairs in common_pairs.items():
        checked_pairs += 1

        try:
            # Dapatkan simbol dari ketujuh bursa
            binance_symbol = exchange_pairs.get("binance")
            kucoin_symbol = exchange_pairs.get("kucoin")
            bybit_symbol = exchange_pairs.get("bybit")
            okx_symbol = exchange_pairs.get("okx")
            gate_symbol = exchange_pairs.get("gate")
            mexc_symbol = exchange_pairs.get("mexc")
            htx_symbol = exchange_pairs.get("htx")

            # Tentukan bursa mana yang tersedia untuk pasangan ini
            available_exchanges = []
            exchange_symbols = {}
            exchange_prices = {}
            exchange_volumes = {}
            exchange_tradable = {}

            # Cek Binance
            if binance_symbol:
                binance_tradable = binance_trading_status.get(binance_symbol, True)
                if binance_tradable:
                    binance_price = binance_prices.get(binance_symbol, 0)
                    binance_volume = binance_volumes.get(binance_symbol, 0)
                    if binance_price > 0 and binance_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("binance")
                        exchange_symbols["binance"] = binance_symbol
                        exchange_prices["binance"] = binance_price
                        exchange_volumes["binance"] = binance_volume
                        exchange_tradable["binance"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di Binance: Harga={binance_price}, Volume=${binance_volume:.2f}")
                else:
                    logger.debug(f"Simbol {binance_symbol} tidak dapat diperdagangkan di Binance")

            # Cek KuCoin
            if kucoin_symbol:
                kucoin_tradable = kucoin_trading_status.get(kucoin_symbol, True)
                if kucoin_tradable:
                    kucoin_price = kucoin_prices.get(kucoin_symbol, 0)
                    kucoin_volume = kucoin_volumes.get(kucoin_symbol, 0)
                    if kucoin_price > 0 and kucoin_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("kucoin")
                        exchange_symbols["kucoin"] = kucoin_symbol
                        exchange_prices["kucoin"] = kucoin_price
                        exchange_volumes["kucoin"] = kucoin_volume
                        exchange_tradable["kucoin"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di KuCoin: Harga={kucoin_price}, Volume=${kucoin_volume:.2f}")
                else:
                    logger.debug(f"Simbol {kucoin_symbol} tidak dapat diperdagangkan di KuCoin")

            # Cek ByBit
            if bybit_symbol:
                bybit_tradable = bybit_trading_status.get(bybit_symbol, True)
                if bybit_tradable:
                    bybit_price = bybit_prices.get(bybit_symbol, 0)
                    bybit_volume = bybit_volumes.get(bybit_symbol, 0)
                    if bybit_price > 0 and bybit_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("bybit")
                        exchange_symbols["bybit"] = bybit_symbol
                        exchange_prices["bybit"] = bybit_price
                        exchange_volumes["bybit"] = bybit_volume
                        exchange_tradable["bybit"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di ByBit: Harga={bybit_price}, Volume=${bybit_volume:.2f}")
                else:
                    logger.debug(f"Simbol {bybit_symbol} tidak dapat diperdagangkan di ByBit")

            # Cek OKX
            if okx_symbol:
                okx_tradable = okx_trading_status.get(okx_symbol, True)
                if okx_tradable:
                    okx_price = okx_prices.get(okx_symbol, 0)
                    okx_volume = okx_volumes.get(okx_symbol, 0)
                    if okx_price > 0 and okx_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("okx")
                        exchange_symbols["okx"] = okx_symbol
                        exchange_prices["okx"] = okx_price
                        exchange_volumes["okx"] = okx_volume
                        exchange_tradable["okx"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di OKX: Harga={okx_price}, Volume=${okx_volume:.2f}")
                else:
                    logger.debug(f"Simbol {okx_symbol} tidak dapat diperdagangkan di OKX")

            # Cek Gate.io
            if gate_symbol:
                gate_tradable = gate_trading_status.get(gate_symbol, True)
                if gate_tradable:
                    gate_price = gate_prices.get(gate_symbol, 0)
                    gate_volume = gate_volumes.get(gate_symbol, 0)
                    if gate_price > 0 and gate_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("gate")
                        exchange_symbols["gate"] = gate_symbol
                        exchange_prices["gate"] = gate_price
                        exchange_volumes["gate"] = gate_volume
                        exchange_tradable["gate"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di Gate.io: Harga={gate_price}, Volume=${gate_volume:.2f}")
                else:
                    logger.debug(f"Simbol {gate_symbol} tidak dapat diperdagangkan di Gate.io")

            # Cek MEXC
            if mexc_symbol:
                mexc_tradable = mexc_trading_status.get(mexc_symbol, True)
                if mexc_tradable:
                    mexc_price = mexc_prices.get(mexc_symbol, 0)
                    mexc_volume = mexc_volumes.get(mexc_symbol, 0)
                    if mexc_price > 0 and mexc_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("mexc")
                        exchange_symbols["mexc"] = mexc_symbol
                        exchange_prices["mexc"] = mexc_price
                        exchange_volumes["mexc"] = mexc_volume
                        exchange_tradable["mexc"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di MEXC: Harga={mexc_price}, Volume=${mexc_volume:.2f}")
                else:
                    logger.debug(f"Simbol {mexc_symbol} tidak dapat diperdagangkan di MEXC")

            # Cek HTX (Huobi)
            if htx_symbol:
                htx_tradable = htx_trading_status.get(htx_symbol, True)
                if htx_tradable:
                    htx_price = htx_prices.get(htx_symbol, 0)
                    htx_volume = htx_volumes.get(htx_symbol, 0)
                    if htx_price > 0 and htx_volume >= MIN_VOLUME_USD:
                        available_exchanges.append("htx")
                        exchange_symbols["htx"] = htx_symbol
                        exchange_prices["htx"] = htx_price
                        exchange_volumes["htx"] = htx_volume
                        exchange_tradable["htx"] = True
                    else:
                        logger.debug(f"Harga atau volume tidak valid untuk {norm_pair} di HTX: Harga={htx_price}, Volume=${htx_volume:.2f}")
                else:
                    logger.debug(f"Simbol {htx_symbol} tidak dapat diperdagangkan di HTX")

            # Kita butuh minimal 2 bursa untuk arbitrase
            if len(available_exchanges) < 2:
                logger.debug(f"Tidak cukup bursa yang tersedia untuk {norm_pair}: {available_exchanges}")
                continue

            # Temukan bursa dengan harga terendah dan tertinggi
            buy_exchange = min(available_exchanges, key=lambda x: exchange_prices[x])
            sell_exchange = max(available_exchanges, key=lambda x: exchange_prices[x])

            # Jika harga sama, lewati
            if exchange_prices[buy_exchange] >= exchange_prices[sell_exchange]:
                logger.debug(f"Tidak ada perbedaan harga untuk {norm_pair}: {buy_exchange}={exchange_prices[buy_exchange]}, {sell_exchange}={exchange_prices[sell_exchange]}")
                continue

            # Dapatkan harga dan volume
            buy_price = exchange_prices[buy_exchange]
            sell_price = exchange_prices[sell_exchange]
            buy_volume = exchange_volumes[buy_exchange]
            sell_volume = exchange_volumes[sell_exchange]

            # Hitung persentase perbedaan harga
            price_diff_pct = ((sell_price - buy_price) / buy_price) * 100

            # Jika perbedaan harga terlalu kecil, lewati
            if price_diff_pct < MIN_PROFIT_THRESHOLD:
                continue

            # Jika perbedaan harga terlalu besar (kemungkinan false positive), lewati
            if price_diff_pct > MAX_PROFIT_THRESHOLD:
                logger.debug(f"Perbedaan harga terlalu besar untuk {norm_pair}: {price_diff_pct:.2f}%")
                continue

            potential_pairs += 1

            # Hitung slippage berdasarkan volume
            buy_price_with_slippage = calculate_slippage(buy_price, buy_volume, "buy")
            sell_price_with_slippage = calculate_slippage(sell_price, sell_volume, "sell")

            # Ekstrak base dan quote asset dari pasangan trading
            if "/" in norm_pair:
                base_asset, quote_asset = norm_pair.split("/")
            else:
                # Fallback jika format tidak sesuai
                base_asset = norm_pair
                quote_asset = "USDT"

            # Hitung jumlah yang bisa dibeli dengan modal
            quantity = MODAL_USD / buy_price_with_slippage

            # Hitung biaya trading
            buy_fee_pct = get_trading_fee(buy_exchange, "taker")
            sell_fee_pct = get_trading_fee(sell_exchange, "taker")

            buy_fee = (quantity * buy_price_with_slippage) * (buy_fee_pct / 100)
            sell_fee = (quantity * sell_price_with_slippage) * (sell_fee_pct / 100)

            # Dapatkan jaringan terbaik untuk base asset
            base_networks = find_common_networks(base_asset)
            best_base_network = find_best_network(base_asset, binance_price, base_networks)

            # Dapatkan jaringan terbaik untuk quote asset
            quote_networks = find_common_networks(quote_asset)
            best_quote_network = find_best_network(quote_asset, binance_price, quote_networks)

            # Pilih jaringan dengan biaya terendah
            if best_base_network["total_fee"] <= best_quote_network["total_fee"]:
                transfer_network = best_base_network
                # Gunakan base_asset untuk transfer
            else:
                transfer_network = best_quote_network
                # Gunakan quote_asset untuk transfer

            # Hitung biaya transfer
            transfer_fee = transfer_network["total_fee"]

            # Hitung nilai setelah jual
            sell_value = (quantity * sell_price_with_slippage) - sell_fee

            # Hitung keuntungan kotor (dalam USD)
            gross_profit_usd = sell_value - (quantity * buy_price_with_slippage) - buy_fee

            # Hitung keuntungan bersih setelah biaya transfer
            net_profit_usd = gross_profit_usd - transfer_fee

            # Hitung ROI
            roi = (net_profit_usd / MODAL_USD) * 100

            # Jika menguntungkan setelah semua biaya
            if net_profit_usd > 0:

                # Buat dictionary untuk menyimpan informasi bursa
                exchange_info = {}
                for exchange in available_exchanges:
                    exchange_info[exchange] = {
                        "symbol": exchange_symbols[exchange],
                        "price": exchange_prices[exchange],
                        "volume": exchange_volumes[exchange],
                        "tradable": exchange_tradable[exchange]
                    }

                opportunity = {
                    "pair": norm_pair,
                    "exchanges": exchange_info,
                    "price_diff_pct": price_diff_pct,
                    "buy_exchange": buy_exchange,
                    "sell_exchange": sell_exchange,
                    "buy_price": buy_price_with_slippage,
                    "sell_price": sell_price_with_slippage,
                    "quantity": quantity,
                    "gross_profit_usd": gross_profit_usd,
                    "net_profit_usd": net_profit_usd,
                    "roi": roi,
                    "base_asset": base_asset,
                    "quote_asset": quote_asset,
                    "base_networks": base_networks,
                    "quote_networks": quote_networks,
                    "best_transfer_network": transfer_network["network"],
                    "withdrawal_fee": transfer_network["withdrawal_fee"],
                    "gas_fee": transfer_network["gas_fee"],
                    "total_transfer_fee": transfer_network["total_fee"],
                    "buy_trading_fee": buy_fee,
                    "sell_trading_fee": sell_fee,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                opportunities.append(opportunity)
                logger.info(
                    f"Peluang arbitrase ditemukan: {norm_pair} - "
                    f"Beli di {buy_exchange.upper()} ({buy_price_with_slippage:.8f}), "
                    f"Jual di {sell_exchange.upper()} ({sell_price_with_slippage:.8f}), "
                    f"Profit: ${gross_profit_usd:.2f}, ROI: {roi:.2f}%"
                )

        except Exception as e:
            logger.error(f"Error menghitung arbitrase untuk {norm_pair}: {e}")

    # Urutkan berdasarkan keuntungan (tertinggi ke terendah)
    opportunities.sort(key=lambda x: x["gross_profit_usd"], reverse=True)

    # Log statistik
    logger.info(
        f"Statistik: Diperiksa {checked_pairs} pasangan, "
        f"{potential_pairs} pasangan potensial, "
        f"{len(opportunities)} peluang arbitrase ditemukan"
    )

    return opportunities


def display_opportunities(opportunities: List[Dict]) -> None:
    """Menampilkan peluang arbitrase"""
    current_time = datetime.now()
    print("\n=== TOP 10 PELUANG ARBITRASE ===")
    print(f"Waktu: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Tampilkan informasi kebaruan data
    binance_data_age = "N/A"
    kucoin_data_age = "N/A"
    bybit_data_age = "N/A"
    okx_data_age = "N/A"
    gate_data_age = "N/A"
    mexc_data_age = "N/A"
    htx_data_age = "N/A"

    if binance_prices_data["timestamp"] is not None:
        binance_age_seconds = (current_time - binance_prices_data["timestamp"]).total_seconds()
        binance_data_age = f"{binance_age_seconds:.1f} detik"

    if kucoin_prices_data["timestamp"] is not None:
        kucoin_age_seconds = (current_time - kucoin_prices_data["timestamp"]).total_seconds()
        kucoin_data_age = f"{kucoin_age_seconds:.1f} detik"

    if bybit_prices_data["timestamp"] is not None:
        bybit_age_seconds = (current_time - bybit_prices_data["timestamp"]).total_seconds()
        bybit_data_age = f"{bybit_age_seconds:.1f} detik"

    if okx_prices_data["timestamp"] is not None:
        okx_age_seconds = (current_time - okx_prices_data["timestamp"]).total_seconds()
        okx_data_age = f"{okx_age_seconds:.1f} detik"

    if gate_prices_data["timestamp"] is not None:
        gate_age_seconds = (current_time - gate_prices_data["timestamp"]).total_seconds()
        gate_data_age = f"{gate_age_seconds:.1f} detik"

    if mexc_prices_data["timestamp"] is not None:
        mexc_age_seconds = (current_time - mexc_prices_data["timestamp"]).total_seconds()
        mexc_data_age = f"{mexc_age_seconds:.1f} detik"

    if htx_prices_data["timestamp"] is not None:
        htx_age_seconds = (current_time - htx_prices_data["timestamp"]).total_seconds()
        htx_data_age = f"{htx_age_seconds:.1f} detik"

    print(f"Data Binance: {binance_data_age} yang lalu | Data KuCoin: {kucoin_data_age} yang lalu | Data ByBit: {bybit_data_age} yang lalu | Data OKX: {okx_data_age} yang lalu")
    print(f"Data Gate.io: {gate_data_age} yang lalu | Data MEXC: {mexc_data_age} yang lalu | Data HTX: {htx_data_age} yang lalu")
    print("=" * 80)

    if not opportunities:
        print("Tidak ada peluang arbitrase ditemukan")
        return

    # Periksa apakah ada pasangan yang tidak dapat diperdagangkan
    non_tradable_pairs = []
    for opp in opportunities:
        exchanges_info = opp.get("exchanges", {})
        for exchange, info in exchanges_info.items():
            if not info.get("tradable", True):
                non_tradable_pairs.append(f"{opp['pair']} di {exchange.upper()}")

    if non_tradable_pairs:
        print("⚠️  PERINGATAN: Beberapa pasangan tidak dapat diperdagangkan:")
        for pair in non_tradable_pairs:
            print(f"   - {pair}")
        print("=" * 80)

    for i, opp in enumerate(opportunities[:10], 1):
        # Tambahkan peringatan jika pasangan tidak dapat diperdagangkan
        exchanges_info = opp.get("exchanges", {})
        warning = ""

        for exchange, info in exchanges_info.items():
            if not info.get("tradable", True):
                warning = " ⚠️"
                break

        print(f"{i}. {opp['pair']}{warning}")
        print(f"   Beli di: {opp['buy_exchange'].upper()} dengan harga {opp['buy_price']:.8f}")
        print(f"   Jual di: {opp['sell_exchange'].upper()} dengan harga {opp['sell_price']:.8f}")
        print(f"   Selisih: {opp['price_diff_pct']:.2f}%")

        # Tampilkan informasi profit dan biaya
        print(f"   Profit Kotor: ${opp['gross_profit_usd']:.2f}")
        print(f"   Biaya Trading Beli: ${opp['buy_trading_fee']:.2f}")
        print(f"   Biaya Trading Jual: ${opp['sell_trading_fee']:.2f}")
        print(f"   Biaya Transfer: ${opp['total_transfer_fee']:.2f} (Penarikan: ${opp['withdrawal_fee']:.2f}, Gas: ${opp['gas_fee']:.2f})")
        print(f"   Profit Bersih: ${opp['net_profit_usd']:.2f}")
        print(f"   ROI: {opp['roi']:.2f}%")

        # Tampilkan informasi status trading untuk setiap bursa
        print("   Status Bursa:")
        for exchange, info in exchanges_info.items():
            status = "✅ Aktif" if info.get("tradable", True) else "❌ Tidak aktif"
            price = info.get("price", 0)
            volume = info.get("volume", 0)
            print(f"     - {exchange.upper()}: {status}, Harga: {price:.8f}, Volume: ${volume:.2f}")

        # Tampilkan informasi jaringan terbaik untuk transfer
        print(f"   Jaringan Transfer Terbaik: {opp['best_transfer_network']}")

        # Tampilkan informasi jaringan untuk base asset
        if "base_networks" in opp and opp["base_networks"]:
            base_networks = opp["base_networks"]
            common_base_networks = base_networks.get("common", [])

            if common_base_networks:
                print(f"   Jaringan {opp['base_asset']} yang didukung semua bursa: {', '.join(common_base_networks)}")
            else:
                # Tampilkan jaringan untuk setiap bursa
                for exchange in ["binance", "kucoin", "bybit"]:
                    if exchange in base_networks:
                        networks = base_networks.get(exchange, [])
                        print(f"   Jaringan {opp['base_asset']} {exchange.upper()}: {', '.join(networks) or 'Tidak ada'}")

        # Tampilkan informasi jaringan untuk quote asset
        if "quote_networks" in opp and opp["quote_networks"]:
            quote_networks = opp["quote_networks"]
            common_quote_networks = quote_networks.get("common", [])

            if common_quote_networks:
                print(f"   Jaringan {opp['quote_asset']} yang didukung semua bursa: {', '.join(common_quote_networks)}")
            else:
                # Tampilkan jaringan untuk setiap bursa
                for exchange in ["binance", "kucoin", "bybit"]:
                    if exchange in quote_networks:
                        networks = quote_networks.get(exchange, [])
                        print(f"   Jaringan {opp['quote_asset']} {exchange.upper()}: {', '.join(networks) or 'Tidak ada'}")

        print("-" * 80)


def save_opportunities(opportunities: List[Dict], filename: str = "arbitrage_opportunities.json") -> None:
    """Menyimpan peluang arbitrase ke file JSON"""
    try:
        with open(filename, "w") as f:
            json.dump(opportunities, f, indent=4)
        logger.info(f"Berhasil menyimpan {len(opportunities)} peluang arbitrase ke {filename}")
    except Exception as e:
        logger.error(f"Error menyimpan peluang arbitrase: {e}")


def signal_handler(sig, _):
    """Menangani sinyal interupsi

    Args:
        sig: Sinyal yang diterima
        _: Frame eksekusi saat sinyal diterima (tidak digunakan)
    """
    global running
    logger.info(f"Menerima sinyal interupsi {sig}, menutup program...")
    running = False
    sys.exit(0)


async def main_loop():
    """Loop utama program"""
    global running

    logger.info("Memulai Final Crypto Arbitrage Scanner...")

    # Selalu ambil data terbaru saat program dimulai
    try:
        # Paksa refresh data dari ketujuh bursa
        logger.info("Mengambil data awal dari ketujuh bursa...")
        binance_prices, binance_volumes = get_binance_prices(force_refresh=True)
        kucoin_prices, kucoin_volumes = get_kucoin_prices(force_refresh=True)
        bybit_prices, bybit_volumes = get_bybit_prices(force_refresh=True)
        okx_prices, okx_volumes = get_okx_prices(force_refresh=True)
        gate_prices, gate_volumes = get_gate_prices(force_refresh=True)
        mexc_prices, mexc_volumes = get_mexc_prices(force_refresh=True)
        htx_prices, htx_volumes = get_htx_prices(force_refresh=True)

        if not binance_prices or not kucoin_prices or not bybit_prices or not okx_prices or not gate_prices or not mexc_prices or not htx_prices:
            logger.error("Gagal mendapatkan data awal dari salah satu bursa")
    except Exception as e:
        logger.error(f"Error saat mengambil data awal: {e}")

    while running:
        try:
            # Dapatkan harga dan volume terbaru dari ketujuh bursa
            # Selalu paksa refresh pada setiap iterasi untuk memastikan data selalu fresh
            binance_prices, binance_volumes = get_binance_prices(force_refresh=True)
            kucoin_prices, kucoin_volumes = get_kucoin_prices(force_refresh=True)
            bybit_prices, bybit_volumes = get_bybit_prices(force_refresh=True)
            okx_prices, okx_volumes = get_okx_prices(force_refresh=True)
            gate_prices, gate_volumes = get_gate_prices(force_refresh=True)
            mexc_prices, mexc_volumes = get_mexc_prices(force_refresh=True)
            htx_prices, htx_volumes = get_htx_prices(force_refresh=True)

            if not binance_prices or not kucoin_prices or not bybit_prices or not okx_prices or not gate_prices or not mexc_prices or not htx_prices:
                logger.error("Gagal mendapatkan harga dari salah satu bursa")
                await asyncio.sleep(10)
                continue

            # Temukan pasangan trading yang sama
            common_pairs = find_common_pairs(
                binance_prices, kucoin_prices, bybit_prices, okx_prices,
                gate_prices, mexc_prices, htx_prices
            )

            # Hitung peluang arbitrase
            opportunities = calculate_arbitrage(
                common_pairs,
                binance_prices, binance_volumes,
                kucoin_prices, kucoin_volumes,
                bybit_prices, bybit_volumes,
                okx_prices, okx_volumes,
                gate_prices, gate_volumes,
                mexc_prices, mexc_volumes,
                htx_prices, htx_volumes
            )

            # Tampilkan peluang
            display_opportunities(opportunities)

            # Simpan peluang ke file
            if opportunities:
                save_opportunities(opportunities)

            # Tunggu sebelum update berikutnya
            logger.info(f"Menunggu {UPDATE_INTERVAL} detik sebelum update berikutnya...")
            await asyncio.sleep(UPDATE_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Program dihentikan oleh pengguna")
            running = False
            break

        except Exception as e:
            logger.error(f"Error tidak tertangani: {e}")
            await asyncio.sleep(10)

    logger.info("Program selesai")


if __name__ == "__main__":
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Jalankan loop asyncio
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Program dihentikan oleh pengguna")
    except Exception as e:
        logger.exception(f"Error tidak tertangani: {e}")
