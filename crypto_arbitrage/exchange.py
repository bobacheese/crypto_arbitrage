#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modul untuk menangani koneksi ke bursa cryptocurrency
"""

import json
import time
import asyncio
import logging
import websockets
import requests
from typing import Dict, List, Set, Optional, Any, Callable
from datetime import datetime

from utils import safe_float, exponential_backoff, normalize_symbol

logger = logging.getLogger("crypto_arbitrage.exchange")

class ExchangeBase:
    """Kelas dasar untuk koneksi ke bursa"""

    def __init__(self, name: str):
        self.name = name
        self.prices = {}
        self.symbols = set()
        self.volumes = {}
        self.order_books = {}  # Cache untuk order book
        self.order_book_timestamps = {}  # Timestamp untuk order book
        self.running = True
        self.last_update = datetime.now()
        self.retry_count = 0
        self.max_retries = 10
        self.ws = None
        self.on_price_update = None  # Callback untuk update harga

    def set_price_update_callback(self, callback: Callable):
        """Set callback untuk update harga"""
        self.on_price_update = callback

    def get_price(self, symbol: str) -> float:
        """Mendapatkan harga untuk simbol tertentu"""
        return safe_float(self.prices.get(symbol, 0.0))

    def get_volume(self, symbol: str) -> float:
        """Mendapatkan volume 24 jam untuk simbol tertentu"""
        return safe_float(self.volumes.get(symbol, 0.0))

    def get_normalized_symbols(self) -> Dict[str, str]:
        """Mendapatkan simbol yang dinormalisasi"""
        return {normalize_symbol(s, self.name): s for s in self.symbols}

    async def get_order_book(self, symbol: str, depth: int = 20, force_refresh: bool = False) -> Dict:
        """Mendapatkan order book untuk simbol tertentu"""
        raise NotImplementedError("Subclass harus mengimplementasikan metode ini")

    def is_order_book_stale(self, symbol: str, max_age_seconds: int = 60) -> bool:
        """Memeriksa apakah order book sudah kedaluwarsa"""
        if symbol not in self.order_book_timestamps:
            return True

        age_seconds = (datetime.now() - self.order_book_timestamps[symbol]).total_seconds()
        return age_seconds > max_age_seconds

    async def connect(self):
        """Menghubungkan ke bursa"""
        raise NotImplementedError("Subclass harus mengimplementasikan metode ini")

    async def disconnect(self):
        """Memutuskan koneksi dari bursa"""
        self.running = False
        if self.ws:
            await self.ws.close()

    def is_connected(self) -> bool:
        """Memeriksa apakah terhubung ke bursa"""
        return self.ws is not None and not self.ws.closed

    def is_stale(self, max_seconds: int = 60) -> bool:
        """Memeriksa apakah data sudah kedaluwarsa"""
        seconds_since_update = (datetime.now() - self.last_update).total_seconds()
        return seconds_since_update > max_seconds


class BinanceExchange(ExchangeBase):
    """Kelas untuk menangani koneksi ke Binance"""

    def __init__(self, ws_url: str, rest_url: str):
        super().__init__("binance")
        self.ws_url = ws_url
        self.rest_url = rest_url
        self.ping_interval = 30  # Binance memerlukan ping setiap 30 detik
        self.connection_timeout = 10  # Timeout untuk koneksi API dalam detik

    async def get_order_book(self, symbol: str, depth: int = 20, force_refresh: bool = False) -> Dict:
        """Mendapatkan order book untuk simbol tertentu"""
        # Cek apakah order book sudah ada di cache dan masih valid
        if not force_refresh and symbol in self.order_books and not self.is_order_book_stale(symbol):
            return self.order_books[symbol]

        try:
            # Ambil order book dari REST API
            response = requests.get(
                f"{self.rest_url}/depth",
                params={"symbol": symbol, "limit": depth},
                timeout=self.connection_timeout
            )
            data = response.json()

            if "bids" in data and "asks" in data:
                # Format order book
                order_book = {
                    "bids": [[price, qty] for price, qty in data["bids"]],
                    "asks": [[price, qty] for price, qty in data["asks"]]
                }

                # Simpan ke cache
                self.order_books[symbol] = order_book
                self.order_book_timestamps[symbol] = datetime.now()

                logger.debug(f"Berhasil mengambil order book Binance untuk {symbol}")
                return order_book
            else:
                logger.error(f"Format order book Binance tidak valid: {data}")
                return {"bids": [], "asks": []}

        except Exception as e:
            logger.error(f"Error mengambil order book Binance untuk {symbol}: {e}")
            return {"bids": [], "asks": []}

    async def fetch_exchange_info(self) -> bool:
        """Mengambil informasi bursa dari REST API"""
        try:
            response = requests.get(f"{self.rest_url}/exchangeInfo", timeout=10)
            data = response.json()

            if "symbols" in data:
                # Filter hanya simbol yang aktif
                active_symbols = [s["symbol"] for s in data["symbols"] if s["status"] == "TRADING"]
                self.symbols.update(active_symbols)
                logger.info(f"Berhasil mengambil {len(active_symbols)} simbol dari Binance REST API")
                return True
            else:
                logger.error(f"Gagal mengambil informasi bursa Binance: {data}")
                return False
        except Exception as e:
            logger.error(f"Error mengambil informasi bursa Binance: {e}")
            return False

    async def fetch_24h_tickers(self) -> bool:
        """Mengambil data ticker 24 jam dari REST API"""
        try:
            response = requests.get(f"{self.rest_url}/ticker/24hr", timeout=10)
            data = response.json()

            for ticker in data:
                symbol = ticker["symbol"]
                price = safe_float(ticker["lastPrice"])
                volume = safe_float(ticker["quoteVolume"])  # Volume dalam mata uang quote

                self.prices[symbol] = price
                self.volumes[symbol] = volume
                self.symbols.add(symbol)

            self.last_update = datetime.now()
            logger.info(f"Berhasil mengambil {len(data)} ticker 24 jam dari Binance REST API")

            # Panggil callback jika ada
            if self.on_price_update:
                self.on_price_update()

            return True
        except Exception as e:
            logger.error(f"Error mengambil ticker 24 jam Binance: {e}")
            return False

    async def ping_websocket(self):
        """Mengirim ping ke WebSocket Binance"""
        while self.running and self.ws and not self.ws.closed:
            try:
                # Binance tidak memerlukan ping eksplisit, tapi kita perlu memeriksa koneksi
                await asyncio.sleep(self.ping_interval)
            except Exception as e:
                logger.error(f"Error dalam ping Binance WebSocket: {e}")
                break

    async def connect(self):
        """Menghubungkan ke WebSocket Binance"""
        # Ambil informasi bursa terlebih dahulu
        await self.fetch_exchange_info()
        await self.fetch_24h_tickers()

        # Berlangganan ke semua ticker
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": ["!ticker@arr"],
            "id": int(time.time() * 1000)
        }

        retry_count = 0
        while self.running and retry_count < self.max_retries:
            try:
                logger.info(f"Menghubungkan ke Binance WebSocket (percobaan ke-{retry_count+1})...")

                async with websockets.connect(self.ws_url) as websocket:
                    self.ws = websocket
                    logger.info("Terhubung ke Binance WebSocket")
                    self.retry_count = 0  # Reset retry counter on successful connection

                    # Kirim pesan berlangganan
                    await websocket.send(json.dumps(subscribe_msg))

                    # Mulai task ping
                    ping_task = asyncio.create_task(self.ping_websocket())

                    while self.running:
                        try:
                            # Set timeout untuk recv
                            response = await asyncio.wait_for(websocket.recv(), timeout=self.ping_interval*2)
                            data = json.loads(response)

                            # Periksa apakah ini adalah respons berlangganan
                            if isinstance(data, dict) and "result" in data:
                                continue

                            # Proses data ticker
                            if isinstance(data, list):
                                update_count = 0
                                for ticker in data:
                                    symbol = ticker["s"]
                                    price = safe_float(ticker["c"])  # Harga penutupan
                                    volume = safe_float(ticker["q"])  # Volume 24 jam dalam quote asset

                                    # Update hanya jika ada perubahan
                                    if symbol not in self.prices or self.prices[symbol] != price:
                                        self.prices[symbol] = price
                                        self.volumes[symbol] = volume
                                        self.symbols.add(symbol)
                                        update_count += 1

                                if update_count > 0:
                                    self.last_update = datetime.now()
                                    logger.debug(f"Diperbarui {update_count} harga Binance")

                                    # Panggil callback jika ada
                                    if self.on_price_update:
                                        self.on_price_update()

                        except asyncio.TimeoutError:
                            logger.warning("Binance WebSocket timeout, mencoba ping...")
                            # Jika timeout, coba kirim ping
                            continue

                        except Exception as e:
                            logger.error(f"Error memproses data Binance: {e}")
                            await asyncio.sleep(1)

                    # Batalkan task ping jika keluar dari loop
                    ping_task.cancel()

            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                retry_count += 1
                delay = exponential_backoff(retry_count)
                logger.error(f"Koneksi Binance WebSocket terputus: {e}. Mencoba lagi dalam {delay:.1f} detik...")
                await asyncio.sleep(delay)

                # Coba ambil data dari REST API saat reconnect
                await self.fetch_24h_tickers()

            except Exception as e:
                retry_count += 1
                delay = exponential_backoff(retry_count)
                logger.error(f"Error koneksi Binance WebSocket: {e}. Mencoba lagi dalam {delay:.1f} detik...")
                await asyncio.sleep(delay)

        if retry_count >= self.max_retries:
            logger.critical(f"Gagal terhubung ke Binance WebSocket setelah {self.max_retries} percobaan")


class KuCoinExchange(ExchangeBase):
    """Kelas untuk menangani koneksi ke KuCoin"""

    def __init__(self, api_url: str):
        super().__init__("kucoin")
        self.api_url = api_url
        self.ws_url = None
        self.ping_interval = 30  # Default, akan diperbarui dari respons API
        self.token = None
        self.connection_timeout = 10  # Timeout untuk koneksi API dalam detik

    async def get_order_book(self, symbol: str, depth: int = 20, force_refresh: bool = False) -> Dict:
        """Mendapatkan order book untuk simbol tertentu"""
        # Cek apakah order book sudah ada di cache dan masih valid
        if not force_refresh and symbol in self.order_books and not self.is_order_book_stale(symbol):
            return self.order_books[symbol]

        try:
            # Ambil order book dari REST API
            response = requests.get(
                f"{self.api_url}/api/v1/market/orderbook/level2_20",
                params={"symbol": symbol},
                timeout=self.connection_timeout
            )
            data = response.json()

            if data["code"] == "200000" and "data" in data:
                # Format order book
                order_book = {
                    "bids": [[price, qty] for price, qty in data["data"]["bids"]],
                    "asks": [[price, qty] for price, qty in data["data"]["asks"]]
                }

                # Simpan ke cache
                self.order_books[symbol] = order_book
                self.order_book_timestamps[symbol] = datetime.now()

                logger.debug(f"Berhasil mengambil order book KuCoin untuk {symbol}")
                return order_book
            else:
                logger.error(f"Format order book KuCoin tidak valid: {data}")
                return {"bids": [], "asks": []}

        except Exception as e:
            logger.error(f"Error mengambil order book KuCoin untuk {symbol}: {e}")
            return {"bids": [], "asks": []}

    async def get_ws_token(self) -> bool:
        """Mendapatkan token untuk koneksi WebSocket KuCoin"""
        try:
            response = requests.post(f"{self.api_url}/api/v1/bullet-public", timeout=10)
            data = response.json()

            if data["code"] == "200000":
                self.token = data["data"]["token"]
                server = data["data"]["instanceServers"][0]
                self.ws_url = f"{server['endpoint']}?token={self.token}&[connectId={int(time.time()*1000)}]"
                self.ping_interval = int(server["pingInterval"] / 1000)
                logger.info(f"Berhasil mendapatkan token KuCoin WebSocket, ping interval: {self.ping_interval}s")
                return True
            else:
                logger.error(f"Gagal mendapatkan token KuCoin: {data}")
                return False
        except Exception as e:
            logger.error(f"Error mendapatkan token KuCoin: {e}")
            return False

    async def fetch_symbols(self) -> bool:
        """Mengambil daftar simbol dari REST API"""
        try:
            response = requests.get(f"{self.api_url}/api/v1/symbols", timeout=10)
            data = response.json()

            if data["code"] == "200000":
                # Filter hanya simbol yang aktif
                active_symbols = [s["symbol"] for s in data["data"] if s["enableTrading"]]
                self.symbols.update(active_symbols)
                logger.info(f"Berhasil mengambil {len(active_symbols)} simbol dari KuCoin REST API")
                return True
            else:
                logger.error(f"Gagal mengambil simbol KuCoin: {data}")
                return False
        except Exception as e:
            logger.error(f"Error mengambil simbol KuCoin: {e}")
            return False

    async def fetch_tickers(self) -> bool:
        """Mengambil data ticker dari REST API"""
        try:
            response = requests.get(f"{self.api_url}/api/v1/market/allTickers", timeout=10)
            data = response.json()

            if data["code"] == "200000":
                for ticker in data["data"]["ticker"]:
                    symbol = ticker["symbol"]
                    price = safe_float(ticker["last"])
                    volume = safe_float(ticker["volValue"])  # Volume dalam USD

                    self.prices[symbol] = price
                    self.volumes[symbol] = volume
                    self.symbols.add(symbol)

                self.last_update = datetime.now()
                logger.info(f"Berhasil mengambil {len(data['data']['ticker'])} ticker dari KuCoin REST API")

                # Panggil callback jika ada
                if self.on_price_update:
                    self.on_price_update()

                return True
            else:
                logger.error(f"Gagal mengambil ticker KuCoin: {data}")
                return False
        except Exception as e:
            logger.error(f"Error mengambil ticker KuCoin: {e}")
            return False

    async def ping_websocket(self, websocket):
        """Mengirim ping ke server KuCoin untuk menjaga koneksi"""
        while self.running and websocket and not websocket.closed:
            try:
                await websocket.send(json.dumps({"type": "ping"}))
                await asyncio.sleep(self.ping_interval)
            except Exception as e:
                logger.error(f"Error mengirim ping ke KuCoin: {e}")
                break

    async def connect(self):
        """Menghubungkan ke WebSocket KuCoin"""
        # Ambil data simbol dan ticker terlebih dahulu
        await self.fetch_symbols()
        await self.fetch_tickers()

        retry_count = 0
        while self.running and retry_count < self.max_retries:
            try:
                # Dapatkan token WebSocket jika belum ada
                if not self.ws_url:
                    success = await self.get_ws_token()
                    if not success:
                        retry_count += 1
                        delay = exponential_backoff(retry_count)
                        logger.error(f"Gagal mendapatkan token KuCoin. Mencoba lagi dalam {delay:.1f} detik...")
                        await asyncio.sleep(delay)
                        continue

                logger.info(f"Menghubungkan ke KuCoin WebSocket (percobaan ke-{retry_count+1})...")

                async with websockets.connect(self.ws_url) as websocket:
                    self.ws = websocket
                    logger.info("Terhubung ke KuCoin WebSocket")
                    self.retry_count = 0  # Reset retry counter on successful connection

                    # Mulai task ping
                    ping_task = asyncio.create_task(self.ping_websocket(websocket))

                    # Berlangganan ke semua ticker
                    subscribe_msg = {
                        "id": int(time.time() * 1000),
                        "type": "subscribe",
                        "topic": "/market/ticker:all",
                        "privateChannel": False,
                        "response": True
                    }

                    await websocket.send(json.dumps(subscribe_msg))

                    while self.running:
                        try:
                            # Set timeout untuk recv
                            response = await asyncio.wait_for(websocket.recv(), timeout=self.ping_interval*2)
                            data = json.loads(response)

                            # Periksa tipe pesan
                            if data.get("type") == "message" and data.get("topic") == "/market/ticker:all":
                                symbol = data["subject"]
                                price = safe_float(data["data"]["price"])
                                volume = safe_float(data["data"]["volValue"])

                                # Update hanya jika ada perubahan
                                if symbol not in self.prices or self.prices[symbol] != price:
                                    self.prices[symbol] = price
                                    self.volumes[symbol] = volume
                                    self.symbols.add(symbol)

                                    self.last_update = datetime.now()
                                    logger.debug(f"Diperbarui harga KuCoin untuk {symbol}: {price}")

                                    # Panggil callback jika ada
                                    if self.on_price_update:
                                        self.on_price_update()

                            elif data.get("type") == "pong":
                                # Respons ping, tidak perlu diproses
                                pass

                            elif data.get("type") == "welcome":
                                # Pesan selamat datang, tidak perlu diproses
                                pass

                            elif data.get("type") == "ack":
                                # Konfirmasi langganan
                                logger.info(f"Berhasil berlangganan ke topik KuCoin: {data.get('id')}")

                        except asyncio.TimeoutError:
                            logger.warning("KuCoin WebSocket timeout, mengirim ping...")
                            # Jika timeout, coba kirim ping
                            try:
                                await websocket.send(json.dumps({"type": "ping"}))
                            except:
                                break

                        except Exception as e:
                            logger.error(f"Error memproses data KuCoin: {e}")
                            await asyncio.sleep(1)

                    # Batalkan task ping jika keluar dari loop
                    ping_task.cancel()

            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                retry_count += 1
                delay = exponential_backoff(retry_count)
                logger.error(f"Koneksi KuCoin WebSocket terputus: {e}. Mencoba lagi dalam {delay:.1f} detik...")

                # Reset token untuk mendapatkan yang baru
                self.ws_url = None
                await asyncio.sleep(delay)

                # Coba ambil data dari REST API saat reconnect
                await self.fetch_tickers()

            except Exception as e:
                retry_count += 1
                delay = exponential_backoff(retry_count)
                logger.error(f"Error koneksi KuCoin WebSocket: {e}. Mencoba lagi dalam {delay:.1f} detik...")

                # Reset token untuk mendapatkan yang baru
                self.ws_url = None
                await asyncio.sleep(delay)

        if retry_count >= self.max_retries:
            logger.critical(f"Gagal terhubung ke KuCoin WebSocket setelah {self.max_retries} percobaan")
