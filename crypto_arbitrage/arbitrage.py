#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modul untuk mendeteksi dan menghitung peluang arbitrase
"""

import logging
import threading
from typing import Dict, List, Set, Tuple, Optional
from datetime import datetime

from config import (
    TRADING_FEES, WITHDRAWAL_FEES, SUPPORTED_NETWORKS, MIN_PROFIT_THRESHOLD,
    MAX_PROFIT_THRESHOLD, MIN_VOLUME_USD, ORDER_BOOK_DEPTH
)
from utils import (
    safe_float, calculate_slippage, validate_arbitrage_opportunity,
    find_common_networks, extract_base_quote, get_min_withdrawal_fee_network,
    calculate_accurate_slippage, is_opportunity_expired, validate_price_data
)

logger = logging.getLogger("crypto_arbitrage.arbitrage")

class ArbitrageDetector:
    """Kelas untuk mendeteksi peluang arbitrase antara dua bursa"""

    def __init__(self, binance_exchange, kucoin_exchange, modal_usd: float):
        self.binance = binance_exchange
        self.kucoin = kucoin_exchange
        self.modal_usd = modal_usd
        self.normalized_pairs = {}
        self.arbitrage_opportunities = []
        self.lock = threading.Lock()
        self.min_volume_usd = MIN_VOLUME_USD  # Minimal volume 24 jam dalam USD
        self.max_roi = MAX_PROFIT_THRESHOLD  # Maksimal ROI yang dianggap valid
        self.min_profit_threshold = MIN_PROFIT_THRESHOLD  # Minimal persentase keuntungan
        self.order_book_depth = ORDER_BOOK_DEPTH  # Kedalaman order book untuk perhitungan slippage

    def find_common_pairs(self) -> Dict[str, Dict[str, str]]:
        """Menemukan pasangan trading yang ada di kedua bursa"""
        normalized_binance = self.binance.get_normalized_symbols()
        normalized_kucoin = self.kucoin.get_normalized_symbols()

        common_normalized = set(normalized_binance.keys()) & set(normalized_kucoin.keys())

        with self.lock:
            self.normalized_pairs = {
                norm: {
                    "binance": normalized_binance[norm],
                    "kucoin": normalized_kucoin[norm]
                }
                for norm in common_normalized
            }

        logger.info(f"Ditemukan {len(self.normalized_pairs)} pasangan trading yang sama di kedua bursa")
        return self.normalized_pairs

    def check_network_compatibility(self, base_asset: str, quote_asset: str) -> bool:
        """Memeriksa apakah ada jaringan yang kompatibel untuk transfer aset antar bursa"""
        # Periksa apakah aset ada di daftar jaringan yang didukung
        if base_asset not in SUPPORTED_NETWORKS or quote_asset not in SUPPORTED_NETWORKS:
            return False

        # Periksa apakah ada jaringan yang sama di kedua bursa
        base_networks = find_common_networks(
            base_asset,
            {"binance": SUPPORTED_NETWORKS[base_asset]["binance"]},
            {"kucoin": SUPPORTED_NETWORKS[base_asset]["kucoin"]}
        )

        quote_networks = find_common_networks(
            quote_asset,
            {"binance": SUPPORTED_NETWORKS[quote_asset]["binance"]},
            {"kucoin": SUPPORTED_NETWORKS[quote_asset]["kucoin"]}
        )

        return bool(base_networks) and bool(quote_networks)

    async def calculate_arbitrage(self) -> List[Dict]:
        """Menghitung peluang arbitrase antara Binance dan KuCoin"""
        opportunities = []
        checked_pairs = 0
        potential_pairs = 0

        with self.lock:
            # Buat salinan untuk menghindari race condition
            pairs_to_check = dict(self.normalized_pairs)

        for norm_pair, exchange_pairs in pairs_to_check.items():
            checked_pairs += 1

            try:
                binance_symbol = exchange_pairs["binance"]
                kucoin_symbol = exchange_pairs["kucoin"]

                # Dapatkan harga dari kedua bursa
                binance_price = self.binance.get_price(binance_symbol)
                kucoin_price = self.kucoin.get_price(kucoin_symbol)

                # Dapatkan volume dari kedua bursa
                binance_volume = self.binance.get_volume(binance_symbol)
                kucoin_volume = self.kucoin.get_volume(kucoin_symbol)

                # Validasi data harga
                if not validate_price_data(binance_price) or not validate_price_data(kucoin_price):
                    logger.debug(f"Harga tidak valid untuk {norm_pair}: Binance={binance_price}, KuCoin={kucoin_price}")
                    continue

                # Lewati jika volume terlalu rendah
                if binance_volume < self.min_volume_usd or kucoin_volume < self.min_volume_usd:
                    logger.debug(f"Volume terlalu rendah untuk {norm_pair}: Binance=${binance_volume:.2f}, KuCoin=${kucoin_volume:.2f}")
                    continue

                # Hitung persentase perbedaan harga
                if binance_price > kucoin_price:
                    price_diff_pct = ((binance_price - kucoin_price) / kucoin_price) * 100
                    buy_exchange = "kucoin"
                    sell_exchange = "binance"
                    buy_price = kucoin_price
                    sell_price = binance_price
                    buy_volume = kucoin_volume
                    sell_volume = binance_volume
                else:
                    price_diff_pct = ((kucoin_price - binance_price) / binance_price) * 100
                    buy_exchange = "binance"
                    sell_exchange = "kucoin"
                    buy_price = binance_price
                    sell_price = kucoin_price
                    buy_volume = binance_volume
                    sell_volume = kucoin_volume

                # Jika perbedaan harga terlalu kecil, lewati
                if price_diff_pct < self.min_profit_threshold:
                    continue

                potential_pairs += 1

                # Ekstrak base dan quote asset
                base_asset, quote_asset = extract_base_quote(norm_pair)

                # Periksa kompatibilitas jaringan
                if not self.check_network_compatibility(base_asset, quote_asset):
                    logger.debug(f"Jaringan tidak kompatibel untuk {norm_pair}")
                    continue

                # Dapatkan jaringan yang didukung
                base_networks = find_common_networks(
                    base_asset,
                    {"binance": SUPPORTED_NETWORKS[base_asset]["binance"]},
                    {"kucoin": SUPPORTED_NETWORKS[base_asset]["kucoin"]}
                )

                quote_networks = find_common_networks(
                    quote_asset,
                    {"binance": SUPPORTED_NETWORKS[quote_asset]["binance"]},
                    {"kucoin": SUPPORTED_NETWORKS[quote_asset]["kucoin"]}
                )

                # Pilih jaringan dengan biaya terendah
                best_base_network, base_fee = get_min_withdrawal_fee_network(
                    base_asset, base_networks, WITHDRAWAL_FEES
                )

                best_quote_network, quote_fee = get_min_withdrawal_fee_network(
                    quote_asset, quote_networks, WITHDRAWAL_FEES
                )

                # Dapatkan order book untuk perhitungan slippage yang lebih akurat
                try:
                    # Dapatkan order book dari bursa pembelian
                    buy_symbol = binance_symbol if buy_exchange == "binance" else kucoin_symbol
                    buy_exchange_obj = self.binance if buy_exchange == "binance" else self.kucoin
                    buy_order_book = await buy_exchange_obj.get_order_book(buy_symbol, self.order_book_depth)

                    # Dapatkan order book dari bursa penjualan
                    sell_symbol = binance_symbol if sell_exchange == "binance" else kucoin_symbol
                    sell_exchange_obj = self.binance if sell_exchange == "binance" else self.kucoin
                    sell_order_book = await sell_exchange_obj.get_order_book(sell_symbol, self.order_book_depth)

                    # Hitung jumlah yang bisa dibeli dengan modal
                    quantity = self.modal_usd / buy_price

                    # Hitung slippage berdasarkan order book
                    buy_price_with_slippage = calculate_accurate_slippage(buy_order_book, quantity, "buy")
                    sell_price_with_slippage = calculate_accurate_slippage(sell_order_book, quantity, "sell")

                    # Jika tidak bisa menghitung slippage dari order book, gunakan metode estimasi
                    if buy_price_with_slippage <= 0:
                        buy_price_with_slippage = calculate_slippage(buy_price, buy_volume, "buy")

                    if sell_price_with_slippage <= 0:
                        sell_price_with_slippage = calculate_slippage(sell_price, sell_volume, "sell")
                except Exception as e:
                    logger.debug(f"Error menghitung slippage dari order book untuk {norm_pair}: {e}")
                    # Fallback ke metode estimasi slippage
                    buy_price_with_slippage = calculate_slippage(buy_price, buy_volume, "buy")
                    sell_price_with_slippage = calculate_slippage(sell_price, sell_volume, "sell")

                # Hitung jumlah yang bisa dibeli dengan modal
                quantity = self.modal_usd / buy_price_with_slippage

                # Hitung biaya trading
                buy_fee_pct = TRADING_FEES[buy_exchange]["taker"]
                sell_fee_pct = TRADING_FEES[sell_exchange]["maker"]

                buy_fee_amount = (quantity * buy_price_with_slippage) * (buy_fee_pct / 100)
                sell_fee_amount = (quantity * sell_price_with_slippage) * (sell_fee_pct / 100)

                # Hitung biaya penarikan dalam USD
                withdrawal_fee_usd = (base_fee * buy_price_with_slippage) + quote_fee

                # Hitung nilai setelah jual
                sell_value = (quantity * sell_price_with_slippage) - sell_fee_amount

                # Hitung keuntungan kotor (dalam USD)
                gross_profit_usd = sell_value - (quantity * buy_price_with_slippage) - buy_fee_amount

                # Hitung keuntungan bersih setelah biaya penarikan (dalam USD)
                net_profit_usd = gross_profit_usd - withdrawal_fee_usd

                # Hitung ROI
                roi = (net_profit_usd / self.modal_usd) * 100

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
                        "buy_price": buy_price_with_slippage,
                        "sell_price": sell_price_with_slippage,
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                        "base_network": best_base_network,
                        "quote_network": best_quote_network,
                        "quantity": quantity,
                        "buy_fee": buy_fee_amount,
                        "sell_fee": sell_fee_amount,
                        "withdrawal_fee_usd": withdrawal_fee_usd,
                        "gross_profit_usd": gross_profit_usd,
                        "net_profit_usd": net_profit_usd,
                        "roi": roi,
                        "binance_volume": binance_volume,
                        "kucoin_volume": kucoin_volume,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    # Validasi peluang
                    is_valid, reason = validate_arbitrage_opportunity(opportunity, self.max_roi)

                    # Validasi tambahan: periksa apakah slippage terlalu tinggi
                    buy_slippage_pct = ((buy_price_with_slippage - buy_price) / buy_price) * 100
                    sell_slippage_pct = ((sell_price - sell_price_with_slippage) / sell_price) * 100

                    if buy_slippage_pct > 5.0:  # Slippage pembelian terlalu tinggi (> 5%)
                        is_valid = False
                        reason = f"Slippage pembelian terlalu tinggi: {buy_slippage_pct:.2f}%"

                    if sell_slippage_pct > 5.0:  # Slippage penjualan terlalu tinggi (> 5%)
                        is_valid = False
                        reason = f"Slippage penjualan terlalu tinggi: {sell_slippage_pct:.2f}%"

                    # Validasi tambahan: periksa apakah ROI masih menguntungkan setelah slippage
                    min_profitable_roi = 1.0  # Minimal ROI 1% untuk dianggap menguntungkan
                    if roi < min_profitable_roi:
                        is_valid = False
                        reason = f"ROI terlalu rendah setelah slippage: {roi:.2f}%"

                    if is_valid:
                        opportunities.append(opportunity)
                        logger.info(
                            f"Peluang arbitrase ditemukan: {norm_pair} - "
                            f"Beli di {buy_exchange.upper()} ({buy_price_with_slippage:.8f}), "
                            f"Jual di {sell_exchange.upper()} ({sell_price_with_slippage:.8f}), "
                            f"Profit: ${net_profit_usd:.2f}, ROI: {roi:.2f}%"
                        )
                    else:
                        logger.debug(f"Peluang arbitrase tidak valid untuk {norm_pair}: {reason}")

            except Exception as e:
                logger.error(f"Error menghitung arbitrase untuk {norm_pair}: {e}")

        # Urutkan berdasarkan keuntungan bersih (tertinggi ke terendah)
        opportunities.sort(key=lambda x: x["net_profit_usd"], reverse=True)

        # Simpan top 10 peluang
        with self.lock:
            self.arbitrage_opportunities = opportunities[:10]

        # Log statistik
        logger.info(
            f"Statistik: Diperiksa {checked_pairs} pasangan, "
            f"{potential_pairs} pasangan potensial, "
            f"{len(opportunities)} peluang arbitrase ditemukan"
        )

        return opportunities

    def get_opportunities(self) -> List[Dict]:
        """Mendapatkan peluang arbitrase terkini"""
        with self.lock:
            return list(self.arbitrage_opportunities)

    async def update(self) -> None:
        """Update peluang arbitrase"""
        self.find_common_pairs()
        await self.calculate_arbitrage()
