#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enhanced Crypto Arbitrage Scanner
---------------------------------
Program untuk mendeteksi dan mengeksekusi peluang arbitrase antara Binance dan KuCoin
dengan validasi dan perhitungan yang lebih akurat.
"""

import asyncio
import logging
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

from config import (
    BINANCE_WS_URL, BINANCE_REST_URL, KUCOIN_API_URL,
    MODAL_IDR, DEFAULT_IDR_USD_RATE, ARBITRAGE_UPDATE_INTERVAL,
    UI_REFRESH_RATE, UI_MAX_OPPORTUNITIES
)
from exchange import BinanceExchange, KuCoinExchange
from arbitrage import ArbitrageDetector
from utils import (
    setup_logging, get_exchange_rate, format_currency,
    is_opportunity_expired, validate_arbitrage_opportunity
)

# Setup logging
logger = setup_logging("enhanced_arbitrage.log", logging.INFO, logging.DEBUG)

# Variabel global
running = True
exchanges_ready = False
arbitrage_detector = None
binance = None
kucoin = None
idr_usd_rate = DEFAULT_IDR_USD_RATE
modal_usd = MODAL_IDR / DEFAULT_IDR_USD_RATE

# Kelas UI
class ArbitrageUI:
    """Kelas untuk menangani tampilan UI"""
    
    def __init__(self, binance_exchange, kucoin_exchange, arbitrage_detector, idr_rate: float):
        self.binance = binance_exchange
        self.kucoin = kucoin_exchange
        self.arbitrage = arbitrage_detector
        self.idr_rate = idr_rate
        self.console = Console()
        self.layout = self._create_layout()
        self.live = None
        self.last_update = datetime.now()
        self.running = True
    
    def _create_layout(self) -> Layout:
        """Membuat layout untuk UI"""
        layout = Layout(name="root")
        
        # Bagi layout menjadi header dan body
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body")
        )
        
        # Bagi body menjadi opportunities dan status
        layout["body"].split_row(
            Layout(name="opportunities", ratio=3),
            Layout(name="status", ratio=1)
        )
        
        return layout
    
    def _generate_header(self) -> Panel:
        """Membuat panel header"""
        title = Text("ENHANCED CRYPTO ARBITRAGE SCANNER", style="bold cyan")
        subtitle = Text("Binance vs KuCoin", style="italic cyan")
        
        return Panel(
            Text.assemble(title, "\n", subtitle),
            box=box.DOUBLE,
            border_style="bright_blue",
            title="[bold yellow]Real-time Arbitrage Scanner[/bold yellow]",
            title_align="center"
        )
    
    def _generate_opportunities_table(self) -> Table:
        """Membuat tabel peluang arbitrase"""
        table = Table(
            title="Peluang Arbitrase Terkini",
            box=box.SIMPLE_HEAD,
            title_style="bold cyan",
            header_style="bold magenta",
            border_style="bright_blue",
            expand=True
        )
        
        # Tambahkan kolom
        table.add_column("Pasangan", style="cyan")
        table.add_column("Beli di", style="green")
        table.add_column("Jual di", style="red")
        table.add_column("Selisih %", justify="right", style="yellow")
        table.add_column("Jaringan", style="blue")
        table.add_column("Profit (USD)", justify="right", style="green")
        table.add_column("Profit (IDR)", justify="right", style="green bold")
        table.add_column("ROI %", justify="right", style="cyan bold")
        
        # Dapatkan peluang arbitrase
        opportunities = self.arbitrage.get_opportunities()
        
        if not opportunities:
            table.add_row(
                "Tidak ada peluang arbitrase ditemukan",
                "", "", "", "", "", "", "",
                style="italic"
            )
        else:
            # Tambahkan baris untuk setiap peluang
            for opp in opportunities[:UI_MAX_OPPORTUNITIES]:
                # Konversi profit ke IDR
                profit_idr = opp["net_profit_usd"] * self.idr_rate
                
                # Periksa apakah peluang sudah kedaluwarsa
                if is_opportunity_expired(opp["timestamp"]):
                    continue
                
                # Validasi peluang
                is_valid, _ = validate_arbitrage_opportunity(opp)
                if not is_valid:
                    continue
                
                table.add_row(
                    opp["pair"],
                    opp["buy_exchange"].upper(),
                    opp["sell_exchange"].upper(),
                    f"{opp['price_diff_pct']:.2f}%",
                    f"{opp['base_asset']}: {opp['base_network']}, {opp['quote_asset']}: {opp['quote_network']}",
                    format_currency(opp["net_profit_usd"], "USD"),
                    format_currency(profit_idr, "IDR"),
                    f"{opp['roi']:.2f}%"
                )
        
        return table
    
    def _generate_status_panel(self) -> Panel:
        """Membuat panel status"""
        binance_status = "✅ Terhubung" if self.binance.is_connected() else "❌ Terputus"
        kucoin_status = "✅ Terhubung" if self.kucoin.is_connected() else "❌ Terputus"
        
        binance_stale = self.binance.is_stale()
        kucoin_stale = self.kucoin.is_stale()
        
        binance_data_status = "❌ Data kedaluwarsa" if binance_stale else "✅ Data terkini"
        kucoin_data_status = "❌ Data kedaluwarsa" if kucoin_stale else "✅ Data terkini"
        
        binance_symbols = len(self.binance.symbols)
        kucoin_symbols = len(self.kucoin.symbols)
        common_pairs = len(self.arbitrage.normalized_pairs)
        
        status_text = Text.assemble(
            Text("Status Koneksi:\n", style="bold"),
            Text(f"Binance: {binance_status}\n", style="green" if self.binance.is_connected() else "red"),
            Text(f"KuCoin: {kucoin_status}\n\n", style="green" if self.kucoin.is_connected() else "red"),
            
            Text("Status Data:\n", style="bold"),
            Text(f"Binance: {binance_data_status}\n", style="green" if not binance_stale else "red"),
            Text(f"KuCoin: {kucoin_data_status}\n\n", style="green" if not kucoin_stale else "red"),
            
            Text("Statistik:\n", style="bold"),
            Text(f"Simbol Binance: {binance_symbols}\n"),
            Text(f"Simbol KuCoin: {kucoin_symbols}\n"),
            Text(f"Pasangan umum: {common_pairs}\n\n"),
            
            Text("Modal:\n", style="bold"),
            Text(f"IDR: {format_currency(MODAL_IDR, 'IDR')}\n"),
            Text(f"USD: {format_currency(modal_usd, 'USD')}\n\n"),
            
            Text("Kurs:\n", style="bold"),
            Text(f"IDR/USD: {self.idr_rate:,.2f}\n\n"),
            
            Text("Waktu:\n", style="bold"),
            Text(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        )
        
        return Panel(
            status_text,
            title="Status",
            border_style="bright_blue",
            box=box.SIMPLE
        )
    
    def update_layout(self) -> None:
        """Update layout dengan data terkini"""
        self.layout["header"].update(self._generate_header())
        self.layout["opportunities"].update(self._generate_opportunities_table())
        self.layout["status"].update(self._generate_status_panel())
    
    def start(self) -> None:
        """Mulai UI dengan Live display"""
        try:
            with Live(self.layout, refresh_per_second=1/UI_REFRESH_RATE, screen=True) as live:
                self.live = live
                
                # Tampilkan pesan selamat datang
                self.console.print("[bold green]Memulai Enhanced Crypto Arbitrage Scanner...[/bold green]")
                self.console.print("[yellow]Menghubungkan ke Binance dan KuCoin...[/yellow]")
                
                # Update layout pertama kali
                self.update_layout()
                
                # Loop utama UI
                while self.running:
                    # Update layout
                    self.update_layout()
                    
                    # Refresh Live display
                    live.refresh()
                    
                    # Tunggu sebelum update berikutnya
                    time.sleep(UI_REFRESH_RATE)
        
        except KeyboardInterrupt:
            logger.info("UI dihentikan oleh pengguna")
            self.running = False
        except Exception as e:
            logger.error(f"Error dalam UI: {e}")
            self.running = False
    
    def stop(self) -> None:
        """Hentikan UI"""
        self.running = False
        if self.live:
            self.live.stop()


class LoadingSpinner:
    """Kelas untuk menampilkan spinner loading"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=self.console
        )
        self.task_id = None
    
    def __enter__(self):
        self.progress.start()
        self.task_id = self.progress.add_task("Menghubungkan ke bursa...", total=None)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()
    
    def update(self, description: str) -> None:
        """Update deskripsi spinner"""
        if self.task_id is not None:
            self.progress.update(self.task_id, description=description)


def signal_handler(sig, frame):
    """Menangani sinyal interupsi"""
    global running
    logger.info("Menerima sinyal interupsi, menutup program...")
    running = False
    sys.exit(0)


async def update_arbitrage_loop(arbitrage_detector, update_interval=ARBITRAGE_UPDATE_INTERVAL):
    """Loop untuk memperbarui peluang arbitrase"""
    global running, exchanges_ready
    
    while running:
        try:
            # Tunggu hingga kedua bursa siap
            if not exchanges_ready:
                await asyncio.sleep(1)
                continue
            
            # Update peluang arbitrase
            await arbitrage_detector.update()
            
            # Tunggu sebelum update berikutnya
            await asyncio.sleep(update_interval)
        
        except Exception as e:
            logger.error(f"Error dalam update arbitrage loop: {e}")
            await asyncio.sleep(5)


async def main():
    """Fungsi utama program"""
    global running, exchanges_ready, arbitrage_detector, binance, kucoin, idr_usd_rate, modal_usd
    
    try:
        logger.info("Memulai Enhanced Crypto Arbitrage Scanner...")
        
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
            await arbitrage_detector.calculate_arbitrage()
            
            # Set flag bahwa bursa sudah siap
            exchanges_ready = True
        
        # Mulai loop update arbitrase dalam task terpisah
        arbitrage_task = asyncio.create_task(
            update_arbitrage_loop(arbitrage_detector)
        )
        
        # Inisialisasi UI
        ui = ArbitrageUI(binance, kucoin, arbitrage_detector, idr_usd_rate)
        
        # Jalankan UI dalam thread terpisah
        ui_thread = threading.Thread(target=ui.start)
        ui_thread.daemon = True
        ui_thread.start()
        
        # Loop utama
        while running:
            # Tunggu sebelum iterasi berikutnya
            await asyncio.sleep(1)
        
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
        if binance:
            await binance.disconnect()
        if kucoin:
            await kucoin.disconnect()
        
        logger.info("Program selesai")


if __name__ == "__main__":
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Jalankan loop asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program dihentikan oleh pengguna")
    except Exception as e:
        logger.exception(f"Error tidak tertangani: {e}")
