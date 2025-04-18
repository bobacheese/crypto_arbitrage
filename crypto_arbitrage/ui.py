#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modul untuk menangani tampilan UI dengan Rich
"""

import time
import logging
from typing import Dict, List, Optional
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

from config import UI_REFRESH_RATE, UI_MAX_OPPORTUNITIES
from utils import format_currency

logger = logging.getLogger("crypto_arbitrage.ui")

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
        title = Text("CRYPTO ARBITRAGE SCANNER", style="bold cyan")
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
                self.console.print("[bold green]Memulai Crypto Arbitrage Scanner...[/bold green]")
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
