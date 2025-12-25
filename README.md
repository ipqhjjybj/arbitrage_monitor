# arbitrage_monitor

## 这是一个黄金套利监控交易系统

1、需要拿到币安如下 PAXG 交易对，每分钟的收盘价、基差、合约持仓量、资金费率、24小时成交量、24小时成交额、合约持仓量、合约持仓金额、订单薄bid和ask累计2盎司的spread, 以及mid_price 这几个字段


2、然后将这些字段每分钟更新一次，输出jsonl增量更新的方式 输出到 多个文件

对于 binance/paxg-future/price.jsonl 
    {
        "timestamp": 1698742800000,
        "price": 4200              # (合约收盘价)
        "mid_price": 4200,         # (合约bid+合约ask)/2
    }

对于 binance/paxg-future/basisRate.jsonl (通过 /futures/data/basis 接口拿到 basisRate)
    {
        "indexPrice": "34400.15945055",
        "contractType": "PERPETUAL",
        "basisRate": "0.0004",
        "futuresPrice": "34414.10",
        "annualizedBasisRate": "",
        "basis": "13.94054945",
        "pair": "PAXGUSDT",
        "timestamp": 1698742800000
    }

对于 binance/paxg-future/openinterest.jsonl (通过/futures/data/openInterestHist 接口拿到，拿5分钟的接口，只拿最新的一条)
    { 
         "symbol":"PAXGUSDT",
	      "sumOpenInterest":"20403.12345678",// 持仓总数量
	      "sumOpenInterestValue": "176196512.12345678", // 持仓总价值
	      "timestamp":1583127900000
    }

对于 binance/paxg-future/fundingRate.jsonl  (通过/fapi/v1/fundingInfo 接口拿到)
    {
        "symbol": "PAXGUSDT",
        "fundingrate": 333,
        "timestamp": 1698742800000
    }

对于 binance/paxg-future/volume_24h.jsonl (通过 /fapi/v1/ticker/24hr 接口拿到)
    {
        "symbol": "PAXGUSDT",
        "volume": "8913.30000000",        //24小时成交量
        "quoteVolume": "15.30000000",     //24小时成交额
    }

对于 binance/paxg-future/spread.jsonl  (通过 /fapi/v1/depth 接口拿到，并计算订单薄bid和ask累计2盎司的spread)
    {
        "symbol": "PAXGUSDT",
        "spread": 233
    }



