#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tes koneksi WebSocket ke Binance dan KuCoin
"""

import asyncio
import json
import websockets
import requests
import logging
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_connection")

# URL API
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
KUCOIN_API_URL = "https://api.kucoin.com"

async def get_kucoin_ws_token():
    """Mendapatkan token untuk koneksi WebSocket KuCoin"""
    try:
        response = requests.post(f"{KUCOIN_API_URL}/api/v1/bullet-public")
        data = response.json()
        if data["code"] == "200000":
            token = data["data"]["token"]
            server = data["data"]["instanceServers"][0]
            ws_url = f"{server['endpoint']}?token={token}&[connectId=test]"
            ping_interval = int(server["pingInterval"] / 1000)
            logger.info(f"KuCoin WebSocket URL: {ws_url}")
            return ws_url, ping_interval
        else:
            logger.error(f"Gagal mendapatkan token KuCoin: {data}")
            return None, None
    except Exception as e:
        logger.error(f"Error mendapatkan token KuCoin: {e}")
        return None, None

async def test_binance():
    """Tes koneksi WebSocket ke Binance"""
    try:
        logger.info("Menghubungkan ke Binance WebSocket...")
        
        # Berlangganan ke ticker BTC/USDT
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": ["btcusdt@ticker"],
            "id": 1
        }
        
        async with websockets.connect(BINANCE_WS_URL) as websocket:
            logger.info("Terhubung ke Binance WebSocket")
            
            # Kirim pesan berlangganan
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("Pesan berlangganan terkirim ke Binance")
            
            # Terima respons berlangganan
            response = await websocket.recv()
            logger.info(f"Respons berlangganan Binance: {response}")
            
            # Terima data ticker
            for _ in range(5):  # Terima 5 pesan
                response = await websocket.recv()
                data = json.loads(response)
                logger.info(f"Data Binance: {data}")
                await asyncio.sleep(1)
            
            logger.info("Tes Binance selesai")
    
    except Exception as e:
        logger.error(f"Error tes Binance: {e}")

async def test_kucoin():
    """Tes koneksi WebSocket ke KuCoin"""
    try:
        logger.info("Mendapatkan token KuCoin...")
        ws_url, ping_interval = await get_kucoin_ws_token()
        
        if not ws_url:
            logger.error("Gagal mendapatkan token KuCoin")
            return
        
        logger.info("Menghubungkan ke KuCoin WebSocket...")
        
        async with websockets.connect(ws_url) as websocket:
            logger.info("Terhubung ke KuCoin WebSocket")
            
            # Berlangganan ke ticker BTC-USDT
            subscribe_msg = {
                "id": int(datetime.now().timestamp() * 1000),
                "type": "subscribe",
                "topic": "/market/ticker:BTC-USDT",
                "privateChannel": False,
                "response": True
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("Pesan berlangganan terkirim ke KuCoin")
            
            # Terima respons berlangganan
            response = await websocket.recv()
            logger.info(f"Respons berlangganan KuCoin: {response}")
            
            # Kirim ping untuk menjaga koneksi
            ping_task = asyncio.create_task(
                send_ping(websocket, ping_interval)
            )
            
            # Terima data ticker
            for _ in range(5):  # Terima 5 pesan
                response = await websocket.recv()
                data = json.loads(response)
                logger.info(f"Data KuCoin: {data}")
                await asyncio.sleep(1)
            
            # Batalkan task ping
            ping_task.cancel()
            logger.info("Tes KuCoin selesai")
    
    except Exception as e:
        logger.error(f"Error tes KuCoin: {e}")

async def send_ping(websocket, interval):
    """Mengirim ping ke server KuCoin untuk menjaga koneksi"""
    try:
        while True:
            await asyncio.sleep(interval)
            await websocket.send(json.dumps({"type": "ping"}))
            logger.info("Ping terkirim ke KuCoin")
    except asyncio.CancelledError:
        logger.info("Task ping dibatalkan")
    except Exception as e:
        logger.error(f"Error mengirim ping: {e}")

async def main():
    """Fungsi utama"""
    logger.info("Memulai tes koneksi...")
    
    # Tes Binance
    await test_binance()
    
    # Tes KuCoin
    await test_kucoin()
    
    logger.info("Tes koneksi selesai")

if __name__ == "__main__":
    asyncio.run(main())
