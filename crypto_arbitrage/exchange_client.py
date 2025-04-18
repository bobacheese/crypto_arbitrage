#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modul untuk menangani koneksi ke bursa cryptocurrency
"""

import json
import time
import asyncio
import websockets
import requests
import logging
from typing import Dict, List, Tuple, Optional, Any, Callable

logger = logging.getLogger("crypto_arbitrage.exchange")

class BinanceClient:
    """Klien untuk koneksi ke Binance WebSocket API"""
    
    def __init__(self):
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.rest_url = "https://api.binance.com/api/v3"
        self.prices = {}
        self.symbols = set()
        self.callbacks = []
    
    def register_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Mendaftarkan callback untuk dijalankan saat data baru diterima"""
        self.callbacks.append(callback)
    
    async def get_exchange_info(self) -> Dict[str, Any]:
        """Mendapatkan informasi bursa dari REST API"""
        try:
            response = requests.get(f"{self.rest_url}/exchangeInfo")
            return response.json()
        except Exception as e:
            logger.error(f"Error mendapatkan info bursa Binance: {e}")
            return {}
    
    async def get_all_tickers(self) -> List[Dict[str, Any]]:
        """Mendapatkan semua ticker dari REST API"""
        try:
            response = requests.get(f"{self.rest_url}/ticker/price")
            return response.json()
        except Exception as e:
            logger.error(f"Error mendapatkan ticker Binance: {e}")
            return []
    
    async def connect(self):
        """Menghubungkan ke WebSocket Binance"""
        try:
            # Berlangganan ke semua ticker
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": ["!ticker@arr"],
                "id": 1
            }
            
            async with websockets.connect(self.ws_url) as websocket:
                logger.info("Terhubung ke Binance WebSocket")
                
                # Kirim pesan berlangganan
                await websocket.send(json.dumps(subscribe_msg))
                
                while True:
                    try:
                        response = await websocket.recv()
                        data = json.loads(response)
                        
                        # Periksa apakah ini adalah respons berlangganan
                        if isinstance(data, dict) and "result" in data:
                            continue
                        
                        # Proses data ticker
                        if isinstance(data, list):
                            for ticker in data:
                                symbol = ticker["s"]
                                price = ticker["c"]  # Harga penutupan
                                self.prices[symbol] = price
                                self.symbols.add(symbol)
                            
                            # Panggil semua callback
                            for callback in self.callbacks:
                                try:
                                    callback({"prices": self.prices, "symbols": self.symbols})
                                except Exception as e:
                                    logger.error(f"Error menjalankan callback Binance: {e}")
                    
                    except Exception as e:
                        logger.error(f"Error memproses data Binance: {e}")
                        await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Error koneksi Binance WebSocket: {e}")
            # Coba hubungkan kembali setelah beberapa detik
            await asyncio.sleep(5)
            return await self.connect()

class KuCoinClient:
    """Klien untuk koneksi ke KuCoin WebSocket API"""
    
    def __init__(self):
        self.api_url = "https://api.kucoin.com"
        self.ws_url = None
        self.ping_interval = 30
        self.prices = {}
        self.symbols = set()
        self.callbacks = []
    
    def register_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Mendaftarkan callback untuk dijalankan saat data baru diterima"""
        self.callbacks.append(callback)
    
    async def get_ws_token(self) -> bool:
        """Mendapatkan token untuk koneksi WebSocket"""
        try:
            response = requests.post(f"{self.api_url}/api/v1/bullet-public")
            data = response.json()
            
            if data["code"] == "200000":
                token = data["data"]["token"]
                server = data["data"]["instanceServers"][0]
                self.ws_url = f"{server['endpoint']}?token={token}&[connectId=arbitrage]"
                self.ping_interval = int(server["pingInterval"] / 1000)
                logger.info(f"KuCoin WebSocket URL: {self.ws_url}")
                return True
            else:
                logger.error(f"Gagal mendapatkan token KuCoin: {data}")
                return False
        
        except Exception as e:
            logger.error(f"Error mendapatkan token KuCoin: {e}")
            return False
    
    async def get_all_tickers(self) -> Dict[str, Any]:
        """Mendapatkan semua ticker dari REST API"""
        try:
            response = requests.get(f"{self.api_url}/api/v1/market/allTickers")
            return response.json()
        except Exception as e:
            logger.error(f"Error mendapatkan ticker KuCoin: {e}")
            return {}
    
    async def ping_loop(self, websocket):
        """Mengirim ping ke server untuk menjaga koneksi"""
        while True:
            try:
                await websocket.send(json.dumps({"type": "ping"}))
                await asyncio.sleep(self.ping_interval)
            except Exception as e:
                logger.error(f"Error mengirim ping ke KuCoin: {e}")
                break
    
    async def connect(self):
        """Menghubungkan ke WebSocket KuCoin"""
        try:
            # Dapatkan token WebSocket jika belum ada
            if not self.ws_url:
                success = await self.get_ws_token()
                if not success:
                    logger.error("Gagal mendapatkan token KuCoin WebSocket")
                    await asyncio.sleep(5)
                    return await self.connect()
            
            async with websockets.connect(self.ws_url) as websocket:
                logger.info("Terhubung ke KuCoin WebSocket")
                
                # Mulai task ping
                ping_task = asyncio.create_task(self.ping_loop(websocket))
                
                # Berlangganan ke semua ticker
                subscribe_msg = {
                    "id": int(time.time() * 1000),
                    "type": "subscribe",
                    "topic": "/market/ticker:all",
                    "privateChannel": False,
                    "response": True
                }
                
                await websocket.send(json.dumps(subscribe_msg))
                
                while True:
                    try:
                        response = await websocket.recv()
                        data = json.loads(response)
                        
                        # Periksa tipe pesan
                        if data.get("type") == "message" and data.get("topic") == "/market/ticker:all":
                            symbol = data["subject"]
                            price = data["data"]["price"]
                            
                            self.prices[symbol] = price
                            self.symbols.add(symbol)
                            
                            # Panggil semua callback
                            for callback in self.callbacks:
                                try:
                                    callback({"prices": self.prices, "symbols": self.symbols})
                                except Exception as e:
                                    logger.error(f"Error menjalankan callback KuCoin: {e}")
                        
                        elif data.get("type") == "pong":
                            # Respons ping, tidak perlu diproses
                            pass
                    
                    except Exception as e:
                        logger.error(f"Error memproses data KuCoin: {e}")
                        await asyncio.sleep(1)
                
                # Batalkan task ping jika keluar dari loop
                ping_task.cancel()
        
        except Exception as e:
            logger.error(f"Error koneksi KuCoin WebSocket: {e}")
            # Reset token dan coba hubungkan kembali
            self.ws_url = None
            await asyncio.sleep(5)
            return await self.connect()

async def test_connection():
    """Fungsi untuk menguji koneksi ke bursa"""
    binance = BinanceClient()
    kucoin = KuCoinClient()
    
    # Callback sederhana untuk menampilkan jumlah simbol
    def print_stats(data):
        if "symbols" in data:
            print(f"Jumlah simbol: {len(data['symbols'])}")
    
    binance.register_callback(print_stats)
    kucoin.register_callback(print_stats)
    
    # Jalankan koneksi dalam task terpisah
    binance_task = asyncio.create_task(binance.connect())
    kucoin_task = asyncio.create_task(kucoin.connect())
    
    # Tunggu beberapa detik untuk melihat hasil
    await asyncio.sleep(30)
    
    # Batalkan task
    binance_task.cancel()
    kucoin_task.cancel()

if __name__ == "__main__":
    # Jalankan tes koneksi jika file dijalankan langsung
    asyncio.run(test_connection())
