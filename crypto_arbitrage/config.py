#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Konfigurasi untuk program Crypto Arbitrage Scanner
"""

# Konfigurasi umum
MODAL_IDR = 10_000_000  # 10 juta rupiah
DEFAULT_IDR_USD_RATE = 15000  # Kurs default jika API tidak tersedia
MIN_PROFIT_THRESHOLD = 0.5  # Minimal persentase keuntungan untuk ditampilkan
MAX_PROFIT_THRESHOLD = 100.0  # Maksimal persentase keuntungan (untuk filter false positive)
MIN_VOLUME_USD = 100000  # Minimal volume 24 jam dalam USD
MAX_SLIPPAGE = 1.0  # Maksimal slippage dalam persentase
ORDER_BOOK_DEPTH = 20  # Kedalaman order book untuk perhitungan slippage
CONNECTION_TIMEOUT = 10  # Timeout untuk koneksi API dalam detik
MAX_RETRIES = 3  # Jumlah maksimal percobaan ulang untuk koneksi API
ARBITRAGE_UPDATE_INTERVAL = 5  # Interval update arbitrase dalam detik
OPPORTUNITY_EXPIRY = 300  # Waktu kedaluwarsa peluang arbitrase dalam detik (5 menit)

# URL API
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_REST_URL = "https://api.binance.com/api/v3"
KUCOIN_API_URL = "https://api.kucoin.com"
EXCHANGE_RATE_API = "https://api.exchangerate-api.com/v4/latest/USD"

# Biaya transaksi (dalam persentase)
TRADING_FEES = {
    "binance": {
        "maker": 0.1,
        "taker": 0.1
    },
    "kucoin": {
        "maker": 0.1,
        "taker": 0.1
    }
}

# Kamus jaringan yang didukung untuk penarikan/deposit
# Format: {simbol: {bursa: [jaringan yang didukung]}}
SUPPORTED_NETWORKS = {
    "BTC": {
        "binance": ["BTC", "BEP20", "BEP2"],
        "kucoin": ["BTC", "KCC"]
    },
    "ETH": {
        "binance": ["ETH", "BEP20", "BEP2"],
        "kucoin": ["ETH", "KCC"]
    },
    "USDT": {
        "binance": ["ETH", "TRX", "BEP20", "BEP2", "SOL"],
        "kucoin": ["ETH", "TRX", "KCC", "EOS", "ALGO"]
    },
    "USDC": {
        "binance": ["ETH", "BEP20", "SOL", "AVAX"],
        "kucoin": ["ETH", "KCC", "TRX", "SOL"]
    },
    "BNB": {
        "binance": ["BEP2", "BEP20"],
        "kucoin": ["BEP2", "BEP20"]
    },
    "XRP": {
        "binance": ["XRP", "BEP20"],
        "kucoin": ["XRP"]
    },
    "ADA": {
        "binance": ["ADA", "BEP20"],
        "kucoin": ["ADA"]
    },
    "SOL": {
        "binance": ["SOL", "BEP20"],
        "kucoin": ["SOL"]
    },
    "DOT": {
        "binance": ["DOT", "BEP20"],
        "kucoin": ["DOT"]
    },
    "DOGE": {
        "binance": ["DOGE", "BEP20"],
        "kucoin": ["DOGE"]
    },
    "AVAX": {
        "binance": ["AVAX", "BEP20", "CCHAIN"],
        "kucoin": ["AVAX", "CCHAIN"]
    },
    "MATIC": {
        "binance": ["MATIC", "ETH", "BEP20"],
        "kucoin": ["MATIC", "ETH"]
    },
    "LINK": {
        "binance": ["ETH", "BEP20"],
        "kucoin": ["ETH", "KCC"]
    },
    "UNI": {
        "binance": ["ETH", "BEP20"],
        "kucoin": ["ETH"]
    },
    "ATOM": {
        "binance": ["ATOM", "BEP20"],
        "kucoin": ["ATOM"]
    }
}

# Biaya penarikan untuk setiap jaringan (dalam USD)
WITHDRAWAL_FEES = {
    "BTC": {
        "binance": {"BTC": 0.0005, "BEP20": 0.0000005, "BEP2": 0.0000005},
        "kucoin": {"BTC": 0.0004, "KCC": 0.0000001}
    },
    "ETH": {
        "binance": {"ETH": 0.005, "BEP20": 0.000001, "BEP2": 0.000001},
        "kucoin": {"ETH": 0.004, "KCC": 0.0000001}
    },
    "USDT": {
        "binance": {"ETH": 15, "TRX": 1, "BEP20": 0.8, "BEP2": 0.8, "SOL": 1},
        "kucoin": {"ETH": 10, "TRX": 1, "KCC": 0.1, "EOS": 1, "ALGO": 0.1}
    },
    "USDC": {
        "binance": {"ETH": 15, "BEP20": 0.8, "SOL": 1, "AVAX": 1},
        "kucoin": {"ETH": 10, "KCC": 0.1, "TRX": 1, "SOL": 1}
    },
    "BNB": {
        "binance": {"BEP2": 0.01, "BEP20": 0.01},
        "kucoin": {"BEP2": 0.02, "BEP20": 0.02}
    },
    "XRP": {
        "binance": {"XRP": 0.25, "BEP20": 0.01},
        "kucoin": {"XRP": 0.2}
    },
    "ADA": {
        "binance": {"ADA": 1, "BEP20": 0.01},
        "kucoin": {"ADA": 1}
    },
    "SOL": {
        "binance": {"SOL": 0.01, "BEP20": 0.01},
        "kucoin": {"SOL": 0.01}
    },
    "DOT": {
        "binance": {"DOT": 0.1, "BEP20": 0.01},
        "kucoin": {"DOT": 0.1}
    },
    "DOGE": {
        "binance": {"DOGE": 5, "BEP20": 0.01},
        "kucoin": {"DOGE": 4}
    },
    "AVAX": {
        "binance": {"AVAX": 0.1, "BEP20": 0.01, "CCHAIN": 0.1},
        "kucoin": {"AVAX": 0.1, "CCHAIN": 0.1}
    },
    "MATIC": {
        "binance": {"MATIC": 0.1, "ETH": 15, "BEP20": 0.01},
        "kucoin": {"MATIC": 0.1, "ETH": 10}
    },
    "LINK": {
        "binance": {"ETH": 0.3, "BEP20": 0.01},
        "kucoin": {"ETH": 0.25, "KCC": 0.01}
    },
    "UNI": {
        "binance": {"ETH": 0.5, "BEP20": 0.01},
        "kucoin": {"ETH": 0.4}
    },
    "ATOM": {
        "binance": {"ATOM": 0.01, "BEP20": 0.01},
        "kucoin": {"ATOM": 0.01}
    }
}

# Daftar pasangan trading umum untuk dimonitor
COMMON_PAIRS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
    "SOL/USDT", "DOT/USDT", "DOGE/USDT", "AVAX/USDT", "MATIC/USDT",
    "LINK/USDT", "UNI/USDT", "ATOM/USDT", "ETH/BTC", "BNB/BTC",
    "XRP/BTC", "ADA/BTC", "SOL/BTC", "DOT/BTC", "DOGE/BTC"
]

# Pengaturan UI
UI_REFRESH_RATE = 1  # Refresh rate UI dalam detik
UI_MAX_OPPORTUNITIES = 5  # Jumlah maksimal peluang yang ditampilkan
