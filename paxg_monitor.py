#!/usr/bin/env python3
"""
PAXG 黄金套利监控系统
从币安获取 PAXG 合约数据，每分钟更新一次，输出到多个 JSONL 文件
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


class BinanceAPI:
    """币安 API 客户端"""

    SPOT_BASE_URL = "https://api.binance.com"
    FUTURES_BASE_URL = "https://fapi.binance.com"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _request(self, base_url: str, endpoint: str, params: dict = None) -> dict | list:
        """发送 API 请求"""
        url = f"{base_url}{endpoint}"
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()

    async def get_futures_ticker(self, symbol: str) -> dict:
        """获取合约 24hr ticker - /fapi/v1/ticker/24hr"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/fapi/v1/ticker/24hr",
            {"symbol": symbol}
        )

    async def get_futures_orderbook(self, symbol: str, limit: int = 100) -> dict:
        """获取合约订单薄 - /fapi/v1/depth"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/fapi/v1/depth",
            {"symbol": symbol, "limit": limit}
        )

    async def get_futures_kline(self, symbol: str, interval: str = "1m", limit: int = 1) -> list:
        """获取合约 K 线"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit}
        )

    async def get_basis(self, pair: str) -> list:
        """获取基差数据 - /futures/data/basis"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/futures/data/basis",
            {"pair": pair, "contractType": "PERPETUAL", "period": "5m", "limit": 1}
        )

    async def get_open_interest_hist(self, symbol: str) -> list:
        """获取历史持仓量 - /futures/data/openInterestHist"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": "5m", "limit": 1}
        )

    async def get_funding_info(self) -> list:
        """获取资金费率信息 - /fapi/v1/fundingInfo"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/fapi/v1/fundingInfo",
            {}
        )

    async def get_funding_rate(self, symbol: str) -> list:
        """获取资金费率 - /fapi/v1/fundingRate"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": 1}
        )

    async def get_premium_index(self, symbol: str) -> dict:
        """获取标记价格和资金费率 - /fapi/v1/premiumIndex"""
        return await self._request(
            self.FUTURES_BASE_URL,
            "/fapi/v1/premiumIndex",
            {"symbol": symbol}
        )


def calculate_orderbook_spread(orderbook: dict, target_quantity: float) -> dict:
    """
    计算订单薄累计指定数量的 spread

    Args:
        orderbook: 订单薄数据 {"bids": [...], "asks": [...]}
        target_quantity: 目标数量（盎司）

    Returns:
        dict: 包含 bid_price, ask_price, spread, mid_price
    """
    bids = orderbook.get("bids", [])
    asks = orderbook.get("asks", [])

    # 计算累计 target_quantity 盎司的加权平均买价
    bid_qty_sum = Decimal("0")
    bid_value_sum = Decimal("0")
    bid_avg_price = Decimal("0")

    for price, qty in bids:
        price = Decimal(str(price))
        qty = Decimal(str(qty))
        remaining = Decimal(str(target_quantity)) - bid_qty_sum

        if remaining <= 0:
            break

        fill_qty = min(qty, remaining)
        bid_qty_sum += fill_qty
        bid_value_sum += fill_qty * price

    if bid_qty_sum > 0:
        bid_avg_price = bid_value_sum / bid_qty_sum

    # 计算累计 target_quantity 盎司的加权平均卖价
    ask_qty_sum = Decimal("0")
    ask_value_sum = Decimal("0")
    ask_avg_price = Decimal("0")

    for price, qty in asks:
        price = Decimal(str(price))
        qty = Decimal(str(qty))
        remaining = Decimal(str(target_quantity)) - ask_qty_sum

        if remaining <= 0:
            break

        fill_qty = min(qty, remaining)
        ask_qty_sum += fill_qty
        ask_value_sum += fill_qty * price

    if ask_qty_sum > 0:
        ask_avg_price = ask_value_sum / ask_qty_sum

    # 计算 spread
    spread = ask_avg_price - bid_avg_price if ask_avg_price and bid_avg_price else Decimal("0")
    mid_price = (ask_avg_price + bid_avg_price) / 2 if ask_avg_price and bid_avg_price else Decimal("0")

    return {
        "bid_price": float(bid_avg_price),
        "ask_price": float(ask_avg_price),
        "spread": float(spread),
        "mid_price": float(mid_price),
    }


class PAXGMonitor:
    """PAXG 套利监控器"""

    def __init__(self, output_dir: str = "binance/paxg-future", target_oz: float = 2.0):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.target_oz = target_oz
        self.symbol = "PAXGUSDT"
        self.pair = "PAXGUSDT"

        # 定义输出文件
        self.files = {
            "price": self.output_dir / "price.jsonl",
            "basisRate": self.output_dir / "basisRate.jsonl",
            "openinterest": self.output_dir / "openinterest.jsonl",
            "fundingRate": self.output_dir / "fundingRate.jsonl",
            "volume_24h": self.output_dir / "volume_24h.jsonl",
            "spread": self.output_dir / "spread.jsonl",
        }

    async def fetch_all_data(self, api: BinanceAPI) -> dict:
        """并发获取所有数据"""
        tasks = {
            "ticker": api.get_futures_ticker(self.symbol),
            "orderbook": api.get_futures_orderbook(self.symbol, limit=100),
            "kline": api.get_futures_kline(self.symbol),
            "basis": api.get_basis(self.pair),
            "open_interest_hist": api.get_open_interest_hist(self.symbol),
            "funding_rate": api.get_funding_rate(self.symbol),
            "premium_index": api.get_premium_index(self.symbol),
        }

        results = {}
        gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for key, result in zip(tasks.keys(), gathered):
            if isinstance(result, Exception):
                print(f"  Warning: Error fetching {key}: {result}")
                results[key] = None
            else:
                results[key] = result

        return results

    def process_and_write(self, raw_data: dict):
        """处理数据并写入各个文件"""
        timestamp = int(time.time() * 1000)

        # 1. price.jsonl
        self._write_price(raw_data, timestamp)

        # 2. basisRate.jsonl
        self._write_basis_rate(raw_data, timestamp)

        # 3. openinterest.jsonl
        self._write_open_interest(raw_data, timestamp)

        # 4. fundingRate.jsonl
        self._write_funding_rate(raw_data, timestamp)

        # 5. volume_24h.jsonl
        self._write_volume(raw_data, timestamp)

        # 6. spread.jsonl
        self._write_spread(raw_data, timestamp)

    def _write_price(self, raw_data: dict, timestamp: int):
        """写入价格数据"""
        kline = raw_data.get("kline") or [[]]
        orderbook = raw_data.get("orderbook") or {}

        close_price = float(kline[0][4]) if kline and len(kline[0]) > 4 else 0
        spread_info = calculate_orderbook_spread(orderbook, self.target_oz)

        data = {
            "timestamp": timestamp,
            "price": close_price,
            "mid_price": spread_info["mid_price"],
        }
        self._append_jsonl(self.files["price"], data)

    def _write_basis_rate(self, raw_data: dict, timestamp: int):
        """写入基差数据"""
        basis_list = raw_data.get("basis") or []

        if basis_list and len(basis_list) > 0:
            basis = basis_list[0]
            data = {
                "indexPrice": basis.get("indexPrice", ""),
                "contractType": basis.get("contractType", "PERPETUAL"),
                "basisRate": basis.get("basisRate", ""),
                "futuresPrice": basis.get("futuresPrice", ""),
                "annualizedBasisRate": basis.get("annualizedBasisRate", ""),
                "basis": basis.get("basis", ""),
                "pair": self.pair,
                "timestamp": basis.get("timestamp", timestamp),
            }
        else:
            # 如果接口没数据，使用空值
            data = {
                "indexPrice": "",
                "contractType": "PERPETUAL",
                "basisRate": "",
                "futuresPrice": "",
                "annualizedBasisRate": "",
                "basis": "",
                "pair": self.pair,
                "timestamp": timestamp,
            }
        self._append_jsonl(self.files["basisRate"], data)

    def _write_open_interest(self, raw_data: dict, timestamp: int):
        """写入持仓量数据"""
        oi_list = raw_data.get("open_interest_hist") or []

        if oi_list and len(oi_list) > 0:
            oi = oi_list[0]
            data = {
                "symbol": self.symbol,
                "sumOpenInterest": oi.get("sumOpenInterest", ""),
                "sumOpenInterestValue": oi.get("sumOpenInterestValue", ""),
                "timestamp": oi.get("timestamp", timestamp),
            }
        else:
            data = {
                "symbol": self.symbol,
                "sumOpenInterest": "",
                "sumOpenInterestValue": "",
                "timestamp": timestamp,
            }
        self._append_jsonl(self.files["openinterest"], data)

    def _write_funding_rate(self, raw_data: dict, timestamp: int):
        """写入资金费率数据"""
        funding_list = raw_data.get("funding_rate") or []
        premium = raw_data.get("premium_index") or {}

        # 优先使用 premiumIndex 的实时资金费率
        funding_rate = premium.get("lastFundingRate", "")

        if not funding_rate and funding_list:
            funding_rate = funding_list[0].get("fundingRate", "")

        data = {
            "symbol": self.symbol,
            "fundingRate": funding_rate,
            "timestamp": timestamp,
        }
        self._append_jsonl(self.files["fundingRate"], data)

    def _write_volume(self, raw_data: dict, timestamp: int):
        """写入24小时成交量数据"""
        ticker = raw_data.get("ticker") or {}

        data = {
            "symbol": self.symbol,
            "volume": ticker.get("volume", ""),
            "quoteVolume": ticker.get("quoteVolume", ""),
            "timestamp": timestamp,
        }
        self._append_jsonl(self.files["volume_24h"], data)

    def _write_spread(self, raw_data: dict, timestamp: int):
        """写入 spread 数据"""
        orderbook = raw_data.get("orderbook") or {}
        spread_info = calculate_orderbook_spread(orderbook, self.target_oz)

        data = {
            "symbol": self.symbol,
            "spread": spread_info["spread"],
            "timestamp": timestamp,
        }
        self._append_jsonl(self.files["spread"], data)

    def _append_jsonl(self, filepath: Path, data: dict):
        """追加写入 JSONL 文件"""
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

    async def run_once(self):
        """运行一次数据采集"""
        async with aiohttp.ClientSession() as session:
            api = BinanceAPI(session)
            raw_data = await self.fetch_all_data(api)
            self.process_and_write(raw_data)
            return raw_data

    async def run_forever(self, interval_seconds: int = 60):
        """持续运行，每分钟采集一次"""
        print(f"Starting PAXG monitor, interval: {interval_seconds}s")
        print(f"Output directory: {self.output_dir}")
        print(f"Files: {', '.join(self.files.keys())}")
        print("-" * 50)

        async with aiohttp.ClientSession() as session:
            api = BinanceAPI(session)

            while True:
                try:
                    start_time = time.time()
                    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                    raw_data = await self.fetch_all_data(api)
                    self.process_and_write(raw_data)

                    # 打印摘要
                    ticker = raw_data.get("ticker") or {}
                    basis_list = raw_data.get("basis") or [{}]
                    basis = basis_list[0] if basis_list else {}

                    print(f"[{ts}] Data written")
                    print(f"  Price: ${ticker.get('lastPrice', 'N/A')} | "
                          f"Basis: {basis.get('basis', 'N/A')} ({basis.get('basisRate', 'N/A')})")

                    # 等待到下一分钟
                    elapsed = time.time() - start_time
                    sleep_time = max(0, interval_seconds - elapsed)
                    await asyncio.sleep(sleep_time)

                except Exception as e:
                    print(f"Error: {e}")
                    import traceback
                    traceback.print_exc()
                    await asyncio.sleep(5)


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="PAXG 黄金套利监控系统")
    parser.add_argument("-o", "--output", default="binance/paxg-future", help="输出目录")
    parser.add_argument("-i", "--interval", type=int, default=60, help="采集间隔（秒）")
    parser.add_argument("--once", action="store_true", help="只运行一次")
    parser.add_argument("--oz", type=float, default=2.0, help="计算 spread 的目标盎司数")

    args = parser.parse_args()

    monitor = PAXGMonitor(output_dir=args.output, target_oz=args.oz)

    if args.once:
        await monitor.run_once()
        print(f"\nData written to {args.output}/")
        for name, path in monitor.files.items():
            if path.exists():
                with open(path) as f:
                    last_line = f.readlines()[-1]
                print(f"\n{name}.jsonl:")
                print(f"  {last_line.strip()}")
    else:
        await monitor.run_forever(interval_seconds=args.interval)


if __name__ == "__main__":
    asyncio.run(main())
