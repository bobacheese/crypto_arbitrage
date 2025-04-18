#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crypto Arbitrage Scanner - Binance vs KuCoin
--------------------------------------------
Program ini mencari peluang arbitrase antara bursa Binance dan KuCoin.
Menggunakan WebSocket untuk mendapatkan data harga real-time dan
menampilkan peluang arbitrase terbaik dengan detail lengkap.

Alur program:
1. Menghubungkan ke Binance dan KuCoin menggunakan WebSocket
2. Mengunduh data harga semua pasangan trading
3. Menormalisasi nama pasangan trading
4. Mencari peluang arbitrase
5. Menampilkan top 5 peluang dengan detail lengkap
"""

import asyncio
import json
import time
import threading
import websockets
import requests
from typing import Dict, List, Tuple, Set, Optional
import logging
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.box import DOUBLE
from rich.text import Text
from rich import box
from rich.live import Live
from rich.layout import Layout

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("crypto_arbitrage")

# Inisialisasi console Rich
console = Console()

# Konstanta
MODAL_IDR = 10_000_000  # 10 juta rupiah
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
KUCOIN_API_URL = "https://api.kucoin.com"

# Biaya transaksi (dalam persentase)
BINANCE_TRADING_FEE = 0.1  # 0.1%
KUCOIN_TRADING_FEE = 0.1   # 0.1%

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
    # Tambahkan lebih banyak aset dan jaringan yang didukung
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
    # Tambahkan lebih banyak aset dan biaya penarikan
}

class CryptoArbitrage:
    def __init__(self):
        self.binance_prices = {}
        self.kucoin_prices = {}
        self.binance_symbols = set()
        self.kucoin_symbols = set()
        self.normalized_pairs = {}
        self.arbitrage_opportunities = []
        self.kucoin_token = None
        self.kucoin_ws_url = None
        self.kucoin_ping_interval = 30
        self.running = True
        self.lock = threading.Lock()
        self.idr_to_usd_rate = self._get_idr_to_usd_rate()
        self.usd_modal = MODAL_IDR / self.idr_to_usd_rate

    def _get_idr_to_usd_rate(self) -> float:
        """Mendapatkan kurs IDR ke USD terkini"""
        try:
            response = requests.get("https://api.exchangerate-api.com/v4/latest/USD")
            data = response.json()
            return data["rates"]["IDR"]
        except Exception as e:
            logger.error(f"Error mendapatkan kurs IDR/USD: {e}")
            return 15000  # Default fallback rate jika API tidak tersedia

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
            base, quote = "", ""
            for common_quote in ["USDT", "BUSD", "BTC", "ETH", "BNB"]:
                if symbol.endswith(common_quote):
                    base = symbol[:-len(common_quote)]
                    quote = common_quote
                    break
            if not base:
                # Fallback jika tidak ada quote yang cocok
                if len(symbol) > 3:
                    base = symbol[:-3]
                    quote = symbol[-3:]
                else:
                    return symbol
            return f"{base}/{quote}"

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

    def check_network_compatibility(self, base_asset: str, quote_asset: str) -> bool:
        """Memeriksa apakah ada jaringan yang kompatibel untuk transfer aset antar bursa"""
        # Periksa apakah aset ada di daftar jaringan yang didukung
        if base_asset not in SUPPORTED_NETWORKS or quote_asset not in SUPPORTED_NETWORKS:
            return False

        # Periksa apakah ada jaringan yang sama di kedua bursa
        binance_base_networks = set(SUPPORTED_NETWORKS[base_asset]["binance"])
        kucoin_base_networks = set(SUPPORTED_NETWORKS[base_asset]["kucoin"])

        binance_quote_networks = set(SUPPORTED_NETWORKS[quote_asset]["binance"])
        kucoin_quote_networks = set(SUPPORTED_NETWORKS[quote_asset]["kucoin"])

        # Jika ada jaringan yang sama untuk kedua aset, maka kompatibel
        base_compatible = bool(binance_base_networks & kucoin_base_networks)
        quote_compatible = bool(binance_quote_networks & kucoin_quote_networks)

        return base_compatible and quote_compatible

    def get_common_networks(self, asset: str) -> List[str]:
        """Mendapatkan jaringan yang didukung oleh kedua bursa untuk suatu aset"""
        if asset not in SUPPORTED_NETWORKS:
            return []

        binance_networks = set(SUPPORTED_NETWORKS[asset]["binance"])
        kucoin_networks = set(SUPPORTED_NETWORKS[asset]["kucoin"])

        return list(binance_networks & kucoin_networks)

    def calculate_withdrawal_fee(self, asset: str, network: str, exchange: str) -> float:
        """Menghitung biaya penarikan untuk aset tertentu"""
        if asset not in WITHDRAWAL_FEES:
            return 0

        if exchange not in WITHDRAWAL_FEES[asset]:
            return 0

        if network not in WITHDRAWAL_FEES[asset][exchange]:
            return 0

        return WITHDRAWAL_FEES[asset][exchange][network]

    def calculate_arbitrage(self):
        """Menghitung peluang arbitrase antara Binance dan KuCoin"""
        opportunities = []

        with self.lock:
            for norm_pair, exchange_pairs in self.normalized_pairs.items():
                binance_symbol = exchange_pairs["binance"]
                kucoin_symbol = exchange_pairs["kucoin"]

                if binance_symbol not in self.binance_prices or kucoin_symbol not in self.kucoin_prices:
                    continue

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

                # Jika perbedaan harga terlalu kecil, lewati
                if price_diff_pct < 0.5:  # Minimal 0.5% perbedaan
                    continue

                # Ekstrak base dan quote asset dari normalized pair
                base_asset, quote_asset = norm_pair.split('/')

                # Periksa kompatibilitas jaringan
                if not self.check_network_compatibility(base_asset, quote_asset):
                    continue

                # Dapatkan jaringan yang didukung
                base_networks = self.get_common_networks(base_asset)
                quote_networks = self.get_common_networks(quote_asset)

                if not base_networks or not quote_networks:
                    continue

                # Pilih jaringan dengan biaya terendah
                best_base_network = min(base_networks,
                                       key=lambda n: (self.calculate_withdrawal_fee(base_asset, n, buy_exchange) +
                                                     self.calculate_withdrawal_fee(base_asset, n, sell_exchange))/2)

                best_quote_network = min(quote_networks,
                                        key=lambda n: (self.calculate_withdrawal_fee(quote_asset, n, buy_exchange) +
                                                      self.calculate_withdrawal_fee(quote_asset, n, sell_exchange))/2)

                # Hitung biaya penarikan
                withdrawal_fee_buy = self.calculate_withdrawal_fee(base_asset, best_base_network, buy_exchange)
                withdrawal_fee_sell = self.calculate_withdrawal_fee(quote_asset, best_quote_network, sell_exchange)

                # Hitung jumlah yang bisa dibeli dengan modal
                quantity = self.usd_modal / buy_price

                # Hitung biaya trading
                buy_fee = (quantity * buy_price) * (BINANCE_TRADING_FEE if buy_exchange == "binance" else KUCOIN_TRADING_FEE) / 100
                sell_fee = (quantity * sell_price) * (BINANCE_TRADING_FEE if sell_exchange == "binance" else KUCOIN_TRADING_FEE) / 100

                # Hitung nilai setelah jual
                sell_value = (quantity * sell_price) - sell_fee

                # Hitung keuntungan kotor (dalam USD)
                gross_profit_usd = sell_value - (quantity * buy_price) - buy_fee

                # Hitung keuntungan bersih setelah biaya penarikan (dalam USD)
                net_profit_usd = gross_profit_usd - (withdrawal_fee_buy * buy_price) - withdrawal_fee_sell

                # Konversi ke IDR
                gross_profit_idr = gross_profit_usd * self.idr_to_usd_rate
                net_profit_idr = net_profit_usd * self.idr_to_usd_rate

                # Hitung ROI
                roi = (net_profit_usd / self.usd_modal) * 100

                # Jika masih menguntungkan setelah biaya
                if net_profit_usd > 0:
                    opportunity = {
                        "pair": norm_pair,
                        "binance_symbol": binance_symbol,
                        "kucoin_symbol": kucoin_symbol,
                        "binance_price": binance_price,
                        "kucoin_price": kucoin_price,
                        "price_diff_pct": price_diff_pct,
                        "buy_exchange": buy_exchange,
                        "sell_exchange": sell_exchange,
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                        "base_network": best_base_network,
                        "quote_network": best_quote_network,
                        "gross_profit_usd": gross_profit_usd,
                        "net_profit_usd": net_profit_usd,
                        "gross_profit_idr": gross_profit_idr,
                        "net_profit_idr": net_profit_idr,
                        "roi": roi,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    opportunities.append(opportunity)

        # Urutkan berdasarkan keuntungan bersih (tertinggi ke terendah)
        opportunities.sort(key=lambda x: x["net_profit_usd"], reverse=True)

        # Simpan top 5 peluang
        with self.lock:
            self.arbitrage_opportunities = opportunities[:5]

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

                            # Hitung peluang arbitrase dalam thread terpisah
                            threading.Thread(target=self.calculate_arbitrage).start()

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

    def create_arbitrage_table(self) -> Table:
        """Membuat tabel untuk menampilkan peluang arbitrase"""
        table = Table(
            title="Top 5 Peluang Arbitrase Binance vs KuCoin",
            box=box.DOUBLE,
            title_style="bold cyan",
            header_style="bold magenta",
            border_style="bright_blue"
        )

        # Tambahkan kolom
        table.add_column("Pasangan", style="cyan")
        table.add_column("Beli di", style="green")
        table.add_column("Jual di", style="red")
        table.add_column("Selisih %", justify="right", style="yellow")
        table.add_column("Jaringan", style="blue")
        table.add_column("Profit Kotor (IDR)", justify="right", style="green")
        table.add_column("Profit Bersih (IDR)", justify="right", style="green bold")
        table.add_column("ROI %", justify="right", style="cyan bold")

        # Tambahkan baris untuk setiap peluang
        with self.lock:
            for opp in self.arbitrage_opportunities:
                table.add_row(
                    opp["pair"],
                    opp["buy_exchange"].upper(),
                    opp["sell_exchange"].upper(),
                    f"{opp['price_diff_pct']:.2f}%",
                    f"{opp['base_asset']}: {opp['base_network']}, {opp['quote_asset']}: {opp['quote_network']}",
                    f"Rp {opp['gross_profit_idr']:,.2f}",
                    f"Rp {opp['net_profit_idr']:,.2f}",
                    f"{opp['roi']:.2f}%"
                )

        return table

    def create_status_panel(self) -> Panel:
        """Membuat panel status untuk menampilkan informasi koneksi dan statistik"""
        binance_count = len(self.binance_symbols)
        kucoin_count = len(self.kucoin_symbols)
        common_count = len(self.normalized_pairs)

        content = Text()
        content.append("Status Koneksi:\n", style="bold")
        content.append(f"Binance: ", style="cyan")
        content.append("Terhubung" if binance_count > 0 else "Menghubungkan...",
                      style="green bold" if binance_count > 0 else "yellow")
        content.append("\nKuCoin: ", style="cyan")
        content.append("Terhubung" if kucoin_count > 0 else "Menghubungkan...",
                      style="green bold" if kucoin_count > 0 else "yellow")

        content.append("\n\nStatistik:\n", style="bold")
        content.append(f"Simbol Binance: ", style="cyan")
        content.append(f"{binance_count}", style="white")
        content.append(f"\nSimbol KuCoin: ", style="cyan")
        content.append(f"{kucoin_count}", style="white")
        content.append(f"\nPasangan Umum: ", style="cyan")
        content.append(f"{common_count}", style="white")
        content.append(f"\nModal: ", style="cyan")
        content.append(f"Rp {MODAL_IDR:,}", style="green bold")
        content.append(f"\nKurs IDR/USD: ", style="cyan")
        content.append(f"{self.idr_to_usd_rate:,.2f}", style="white")
        content.append(f"\nUpdate Terakhir: ", style="cyan")
        content.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="white")

        return Panel(
            content,
            title="Informasi Sistem",
            border_style="bright_blue",
            box=box.ROUNDED
        )

    def display_ui(self):
        """Menampilkan UI dengan Rich"""
        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=1)
        )

        layout["main"].split_row(
            Layout(name="table", ratio=3),
            Layout(name="status", ratio=1)
        )

        # Header
        layout["header"].update(Panel(
            Text("CRYPTO ARBITRAGE SCANNER - BINANCE vs KUCOIN", justify="center", style="bold white on blue"),
            box=box.HEAVY,
            border_style="bright_blue"
        ))

        # Footer
        layout["footer"].update(Text(
            "Tekan Ctrl+C untuk keluar | Dibuat dengan Rich dan Python | Data real-time dari Binance dan KuCoin",
            justify="center",
            style="italic bright_black"
        ))

        with Live(layout, refresh_per_second=1, screen=True):
            while self.running:
                try:
                    # Update tabel arbitrase
                    layout["table"].update(self.create_arbitrage_table())

                    # Update panel status
                    layout["status"].update(self.create_status_panel())

                    time.sleep(1)
                except KeyboardInterrupt:
                    self.running = False
                except Exception as e:
                    logger.error(f"Error menampilkan UI: {e}")
                    time.sleep(1)

    async def run(self):
        """Menjalankan program arbitrase"""
        try:
            # Mulai task WebSocket
            binance_task = asyncio.create_task(self.binance_websocket())
            kucoin_task = asyncio.create_task(self.kucoin_websocket())

            # Mulai thread UI
            ui_thread = threading.Thread(target=self.display_ui)
            ui_thread.daemon = True
            ui_thread.start()

            # Tunggu hingga program dihentikan
            while self.running:
                await asyncio.sleep(1)

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
        # Aktifkan logging ke konsol
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Banner
        console.print(Panel.fit(
            Text("CRYPTO ARBITRAGE SCANNER", justify="center", style="bold white"),
            subtitle=Text("Binance vs KuCoin", justify="center", style="italic cyan"),
            box=DOUBLE,
            border_style="bright_blue"
        ))

        console.print("\n[bold green]Memulai program arbitrase...[/bold green]")
        console.print("[yellow]Menghubungkan ke Binance dan KuCoin...[/yellow]")

        # Jalankan program
        arbitrage = CryptoArbitrage()
        logger.info("Inisialisasi CryptoArbitrage selesai")
        logger.info(f"Kurs IDR/USD: {arbitrage.idr_to_usd_rate}")
        logger.info(f"Modal USD: {arbitrage.usd_modal}")

        # Jalankan program dalam mode verbose
        console.print("\n[bold]Program berjalan dalam mode verbose. Tekan Ctrl+C untuk keluar.[/bold]")
        asyncio.run(arbitrage.run())

    except KeyboardInterrupt:
        console.print("\n[bold red]Program dihentikan oleh pengguna[/bold red]")
    except Exception as e:
        logger.exception("Error tidak tertangani")
        console.print(f"\n[bold red]Error: {e}[/bold red]")
