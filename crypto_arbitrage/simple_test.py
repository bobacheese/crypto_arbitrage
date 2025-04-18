#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Program sederhana untuk menguji koneksi ke bursa
"""

import requests
import json
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("simple_test")

# URL API
BINANCE_REST_URL = "https://api.binance.com/api/v3"
KUCOIN_API_URL = "https://api.kucoin.com"

def test_binance():
    """Menguji koneksi ke Binance"""
    print("Menguji koneksi ke Binance...")
    
    try:
        # Ambil informasi bursa
        print("Mengambil informasi bursa Binance...")
        response = requests.get(f"{BINANCE_REST_URL}/exchangeInfo", timeout=10)
        data = response.json()
        
        if "symbols" in data:
            active_symbols = [s["symbol"] for s in data["symbols"] if s["status"] == "TRADING"]
            print(f"Berhasil mengambil {len(active_symbols)} simbol dari Binance")
            
            # Ambil beberapa harga
            if active_symbols:
                symbol = active_symbols[0]
                print(f"Mengambil harga untuk {symbol}...")
                response = requests.get(f"{BINANCE_REST_URL}/ticker/price?symbol={symbol}", timeout=10)
                price_data = response.json()
                print(f"Harga {symbol}: {price_data['price']}")
                
                return True
        else:
            print(f"Gagal mengambil informasi bursa Binance: {data}")
            return False
    
    except Exception as e:
        print(f"Error menguji koneksi ke Binance: {e}")
        return False

def test_kucoin():
    """Menguji koneksi ke KuCoin"""
    print("Menguji koneksi ke KuCoin...")
    
    try:
        # Ambil daftar simbol
        print("Mengambil daftar simbol KuCoin...")
        response = requests.get(f"{KUCOIN_API_URL}/api/v1/symbols", timeout=10)
        data = response.json()
        
        if data["code"] == "200000":
            active_symbols = [s["symbol"] for s in data["data"] if s["enableTrading"]]
            print(f"Berhasil mengambil {len(active_symbols)} simbol dari KuCoin")
            
            # Ambil beberapa harga
            if active_symbols:
                symbol = active_symbols[0]
                print(f"Mengambil harga untuk {symbol}...")
                response = requests.get(f"{KUCOIN_API_URL}/api/v1/market/orderbook/level1?symbol={symbol}", timeout=10)
                price_data = response.json()
                if price_data["code"] == "200000":
                    print(f"Harga {symbol}: {price_data['data']['price']}")
                    
                    return True
                else:
                    print(f"Gagal mengambil harga KuCoin: {price_data}")
                    return False
        else:
            print(f"Gagal mengambil simbol KuCoin: {data}")
            return False
    
    except Exception as e:
        print(f"Error menguji koneksi ke KuCoin: {e}")
        return False

def main():
    """Fungsi utama"""
    print("Memulai pengujian koneksi ke bursa...")
    
    # Uji koneksi ke Binance
    binance_ok = test_binance()
    print(f"Koneksi ke Binance: {'OK' if binance_ok else 'GAGAL'}")
    
    # Uji koneksi ke KuCoin
    kucoin_ok = test_kucoin()
    print(f"Koneksi ke KuCoin: {'OK' if kucoin_ok else 'GAGAL'}")
    
    # Hasil
    if binance_ok and kucoin_ok:
        print("Pengujian berhasil: Kedua bursa dapat diakses")
    else:
        print("Pengujian gagal: Tidak semua bursa dapat diakses")

if __name__ == "__main__":
    main()
