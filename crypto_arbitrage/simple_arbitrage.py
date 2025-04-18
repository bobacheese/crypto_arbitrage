#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Versi sederhana dari program Crypto Arbitrage Scanner
"""

import asyncio
import json
import time
import threading
import websockets
import requests
import logging
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich import box

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("arbitrage.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("simple_arbitrage")

# Inisialisasi console Rich
console = Console()

# Konstanta
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
KUCOIN_API_URL = "https://api.kucoin.com"
TRADING_FEE = 0.1  # 0.1%
MODAL_USD = 1000  # 1000 USD

class SimpleArbitrage:
    def __init__(self):
        self.binance_prices = {}
        self.kucoin_prices = {}
        self.binance_symbols = set()
        self.kucoin_symbols = set()
        self.normalized_pairs = {}
        self.arbitrage_opportunities = []
        self.kucoin_ws_url = None
        self.kucoin_ping_interval = 30
        self.running = True
        self.lock = threading.Lock()

    async def get_kucoin_ws_token(self):
        """Mendapatkan token untuk koneksi WebSocket KuCoin"""
        try:
            response = requests.post(f"{KUCOIN_API_URL}/api/v1/bullet-public")
            data = response.json()
            if data["code"] == "200000":
                token = data["data"]["token"]
                server = data["data"]["instanceServers"][0]
                self.kucoin_ws_url = f"{server['endpoint']}?token={token}&[connectId=arbitrage]"
                self.kucoin_ping_interval = int(server["pingInterval"] / 1000)
                logger.info(f"KuCoin WebSocket URL: {self.kucoin_ws_url}")
                return True
            else:
                logger.error(f"Gagal mendapatkan token KuCoin: {data}")
                return False
        except Exception as e:
            logger.error(f"Error mendapatkan token KuCoin: {e}")
            return False

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Menormalisasi nama simbol untuk konsistensi antar bursa"""
        if exchange == "binance":
            # Binance format: BTCUSDT
            common_quotes = ["USDT", "BUSD", "BTC", "ETH", "BNB"]
            for quote in common_quotes:
                if symbol.endswith(quote):
                    base = symbol[:-len(quote)]
                    return f"{base}/{quote}"
            return symbol

        elif exchange == "kucoin":
            # KuCoin format: BTC-USDT
            if "-" in symbol:
                base, quote = symbol.split("-")
                return f"{base}/{quote}"
            return symbol

        return symbol

    def find_common_pairs(self):
        """Menemukan pasangan trading yang ada di kedua bursa"""
        normalized_binance = {self.normalize_symbol(s, "binance"): s for s in self.binance_symbols}
        normalized_kucoin = {self.normalize_symbol(s, "kucoin"): s for s in self.kucoin_symbols}

        common_normalized = set(normalized_binance.keys()) & set(normalized_kucoin.keys())

        self.normalized_pairs = {
            norm: {
                "binance": normalized_binance[norm],
                "kucoin": normalized_kucoin[norm]
            }
            for norm in common_normalized
        }

        logger.info(f"Ditemukan {len(self.normalized_pairs)} pasangan trading yang sama di kedua bursa")

    def calculate_arbitrage(self):
        """Menghitung peluang arbitrase antara Binance dan KuCoin"""
        opportunities = []
        checked_pairs = 0
        potential_pairs = 0

        with self.lock:
            for norm_pair, exchange_pairs in self.normalized_pairs.items():
                binance_symbol = exchange_pairs["binance"]
                kucoin_symbol = exchange_pairs["kucoin"]

                if binance_symbol not in self.binance_prices or kucoin_symbol not in self.kucoin_prices:
                    continue

                checked_pairs += 1

                try:
                    binance_price = float(self.binance_prices[binance_symbol])
                    kucoin_price = float(self.kucoin_prices[kucoin_symbol])

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

                    # Log semua pasangan dengan perbedaan harga
                    logger.info(f"Perbedaan harga: {norm_pair} - {buy_exchange.upper()}:{buy_price} vs {sell_exchange.upper()}:{sell_price}, Selisih: {price_diff_pct:.4f}%")

                    # Jika perbedaan harga terlalu kecil, lewati untuk perhitungan arbitrase
                    if price_diff_pct < 0.05:  # Minimal 0.05% perbedaan untuk arbitrase
                        continue

                    potential_pairs += 1

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

                    # Jika menguntungkan
                    if gross_profit_usd > 0:
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
                        logger.info(f"Peluang arbitrase ditemukan: {norm_pair} - Beli di {buy_exchange.upper()} ({buy_price}), Jual di {sell_exchange.upper()} ({sell_price}), Profit: ${gross_profit_usd:.2f}, ROI: {roi:.2f}%")
                except Exception as e:
                    logger.error(f"Error menghitung arbitrase untuk {norm_pair}: {e}")

        # Urutkan berdasarkan keuntungan (tertinggi ke terendah)
        opportunities.sort(key=lambda x: x["gross_profit_usd"], reverse=True)

        # Simpan top 5 peluang
        with self.lock:
            self.arbitrage_opportunities = opportunities[:5]

            # Tampilkan peluang
            if self.arbitrage_opportunities:
                self.display_opportunities()

            # Log statistik
            logger.info(f"Statistik: Diperiksa {checked_pairs} pasangan, {potential_pairs} pasangan potensial, {len(opportunities)} peluang arbitrase ditemukan")

    def display_opportunities(self):
        """Menampilkan peluang arbitrase"""
        table = Table(
            title="Top 5 Peluang Arbitrase",
            box=box.SIMPLE,
            header_style="bold magenta"
        )

        table.add_column("Pasangan", style="cyan")
        table.add_column("Beli di", style="green")
        table.add_column("Jual di", style="red")
        table.add_column("Selisih %", justify="right", style="yellow")
        table.add_column("Profit (USD)", justify="right", style="green")
        table.add_column("ROI %", justify="right", style="cyan")

        for opp in self.arbitrage_opportunities:
            table.add_row(
                opp["pair"],
                opp["buy_exchange"].upper(),
                opp["sell_exchange"].upper(),
                f"{opp['price_diff_pct']:.2f}%",
                f"${opp['gross_profit_usd']:.2f}",
                f"{opp['roi']:.2f}%"
            )

        console.print(table)
        console.print(f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        console.print(f"Jumlah pasangan umum: {len(self.normalized_pairs)}")
        console.print(f"Jumlah simbol Binance: {len(self.binance_symbols)}")
        console.print(f"Jumlah simbol KuCoin: {len(self.kucoin_symbols)}")
        console.print("=" * 80)

    async def binance_websocket(self):
        """Menangani koneksi WebSocket ke Binance"""
        try:
            # Berlangganan ke semua ticker
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": ["!ticker@arr"],
                "id": 1
            }

            async with websockets.connect(BINANCE_WS_URL) as websocket:
                logger.info("Terhubung ke Binance WebSocket")

                # Kirim pesan berlangganan
                await websocket.send(json.dumps(subscribe_msg))

                while self.running:
                    try:
                        response = await websocket.recv()
                        data = json.loads(response)

                        # Periksa apakah ini adalah respons berlangganan
                        if isinstance(data, dict) and "result" in data:
                            continue

                        # Proses data ticker
                        if isinstance(data, list):
                            with self.lock:
                                for ticker in data:
                                    symbol = ticker["s"]
                                    price = ticker["c"]  # Harga penutupan
                                    self.binance_prices[symbol] = price
                                    self.binance_symbols.add(symbol)

                            # Temukan pasangan umum setelah mendapatkan data
                            if self.binance_symbols and self.kucoin_symbols:
                                self.find_common_pairs()

                            # Hitung peluang arbitrase
                            self.calculate_arbitrage()

                    except Exception as e:
                        logger.error(f"Error memproses data Binance: {e}")
                        await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error koneksi Binance WebSocket: {e}")
            # Coba hubungkan kembali setelah beberapa detik
            await asyncio.sleep(5)
            asyncio.create_task(self.binance_websocket())

    async def kucoin_ping(self, websocket):
        """Mengirim ping ke server KuCoin untuk menjaga koneksi"""
        while self.running:
            try:
                await websocket.send(json.dumps({"type": "ping"}))
                await asyncio.sleep(self.kucoin_ping_interval)
            except Exception as e:
                logger.error(f"Error mengirim ping ke KuCoin: {e}")
                break

    async def kucoin_websocket(self):
        """Menangani koneksi WebSocket ke KuCoin"""
        try:
            # Dapatkan token WebSocket jika belum ada
            if not self.kucoin_ws_url:
                success = await self.get_kucoin_ws_token()
                if not success:
                    logger.error("Gagal mendapatkan token KuCoin WebSocket")
                    await asyncio.sleep(5)
                    asyncio.create_task(self.kucoin_websocket())
                    return

            async with websockets.connect(self.kucoin_ws_url) as websocket:
                logger.info("Terhubung ke KuCoin WebSocket")

                # Mulai task ping
                ping_task = asyncio.create_task(self.kucoin_ping(websocket))

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
                        response = await websocket.recv()
                        data = json.loads(response)

                        # Periksa tipe pesan
                        if data.get("type") == "message" and data.get("topic") == "/market/ticker:all":
                            symbol = data["subject"]
                            price = data["data"]["price"]

                            with self.lock:
                                self.kucoin_prices[symbol] = price
                                self.kucoin_symbols.add(symbol)

                            # Temukan pasangan umum setelah mendapatkan data
                            if self.binance_symbols and self.kucoin_symbols:
                                self.find_common_pairs()

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
            self.kucoin_ws_url = None
            await asyncio.sleep(5)
            asyncio.create_task(self.kucoin_websocket())

    async def run(self):
        """Menjalankan program arbitrase"""
        try:
            # Mulai task WebSocket
            binance_task = asyncio.create_task(self.binance_websocket())
            kucoin_task = asyncio.create_task(self.kucoin_websocket())

            # Tunggu hingga program dihentikan
            status_counter = 0
            while self.running:
                await asyncio.sleep(10)
                status_counter += 1

                # Tampilkan status setiap 10 detik
                logger.info(f"Status: Binance symbols={len(self.binance_symbols)}, KuCoin symbols={len(self.kucoin_symbols)}, Common pairs={len(self.normalized_pairs)}")

                # Tampilkan peluang arbitrase setiap 60 detik
                if status_counter % 6 == 0 and self.arbitrage_opportunities:
                    logger.info("Peluang arbitrase terkini:")
                    for i, opp in enumerate(self.arbitrage_opportunities, 1):
                        logger.info(f"{i}. {opp['pair']} - Beli di {opp['buy_exchange'].upper()}, Jual di {opp['sell_exchange'].upper()}, Profit: ${opp['gross_profit_usd']:.2f}, ROI: {opp['roi']:.2f}%")

            # Batalkan task jika program dihentikan
            binance_task.cancel()
            kucoin_task.cancel()

        except KeyboardInterrupt:
            logger.info("Program dihentikan oleh pengguna")
            self.running = False
        except Exception as e:
            logger.error(f"Error menjalankan program: {e}")
            self.running = False

if __name__ == "__main__":
    try:
        console.print("=" * 80)
        console.print("CRYPTO ARBITRAGE SCANNER - VERSI SEDERHANA", style="bold cyan", justify="center")
        console.print("Binance vs KuCoin", style="italic cyan", justify="center")
        console.print("=" * 80)

        console.print("\n[bold green]Memulai program arbitrase...[/bold green]")
        console.print("[yellow]Menghubungkan ke Binance dan KuCoin...[/yellow]")

        # Jalankan program
        arbitrage = SimpleArbitrage()
        asyncio.run(arbitrage.run())

    except KeyboardInterrupt:
        console.print("\n[bold red]Program dihentikan oleh pengguna[/bold red]")
    except Exception as e:
        logger.exception("Error tidak tertangani")
        console.print(f"\n[bold red]Error: {e}[/bold red]")
