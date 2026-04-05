"""
订单还原类
"""
import copy
import dolphindb as ddb
import pandas as pd
import tqdm
from typing import Dict, List

class Position:
    """每个时间戳核算一次 -> 计算每个品种的pnl"""
    def __init__(self):
        self.lastTime: pd.Timestamp = pd.Timestamp("1900-01-01")
        self.currentTime: pd.Timestamp = pd.Timestamp("1900-01-01")
        self.longPos: Dict[str, Dict[str, any]] = {}
        self.shortPos: Dict[str, Dict[str, any]] = {}
        self.longPnl: Dict[str, float] = {}
        self.shortPnl: Dict[str, float] = {}
        self.totalPnl: Dict[str, float] = {}
        self.pnlDF: pd.DataFrame = pd.DataFrame()   # 当前时间戳的pnl
        self.cumPnlDF: pd.DataFrame = pd.DataFrame()    # 累计pnl

    def openPos(self, timestamp: pd.Timestamp, symbol: str, direction: str, price: float, vol: int) -> None:
        """开仓/加仓"""
        self.currentTime = timestamp    # 更新时间
        if self.currentTime != self.lastTime:
            self.lastTime = copy.copy(timestamp)
            self.recordPnl()

        pos = {"vol": vol, "price": price}
        if direction == "long":
            if symbol not in self.longPos:
                self.longPos[symbol] = [pos]
            else:
                self.longPos[symbol].append(pos)
            if symbol not in self.longPnl:
                self.longPnl[symbol] = 0.0
        else:
            if symbol not in self.shortPos:
                self.shortPos[symbol] = [pos]
            else:
                self.shortPos[symbol].append(pos)
            if symbol not in self.shortPnl:
                self.shortPnl[symbol] = 0.0
        if symbol not in self.totalPnl:
            self.totalPnl[symbol] = 0.0

    def closePos(self, timestamp: pd.Timestamp, symbol: str, direction: str, price: float, vol: int) -> None:
        """平仓"""
        pnl: float = 0.0
        flag = 1.0 if direction == "long" else -1
        if direction == "long":
            if symbol not in self.longPos:
                raise ValueError(f"{tradeTime}-{symbol}-{direction}: 该品种缺少开仓交易记录")
            for pos in self.longPos[symbol]:
                if pos["vol"] <= vol:
                    self.longPos[symbol].pop(0)
                    vol -= pos["vol"]
                    pnl += pos["vol"] * (price - pos["price"]) * flag
                else:
                    self.longPos[symbol][0]["vol"] -= vol
                    pnl += vol * (price - pos["price"]) * flag
                    break
            self.longPnl[symbol] += pnl
        else:
            if symbol not in self.shortPos:
                raise ValueError(f"{tradeTime}-{symbol}-{direction}: 该品种缺少开仓交易记录")
            for pos in self.shortPos[symbol]:
                if pos["vol"] <= vol:
                    self.shortPos[symbol].pop(0)
                    vol -= pos["vol"]
                    pnl += pos["vol"] * (pos["price"] - price) * flag
                else:
                    self.shortPos[symbol][0]["vol"] -= vol
                    pnl += vol * (pos["price"] - price) * flag
                    break
            self.shortPos[symbol] += pnl
        self.totalPnl[symbol] += pnl

    def recordPnl(self) -> None:
        """输出当前pnl为DataFrame格式并追加"""


class Simulator:
    def __init__(self, session: ddb.session, precision: float = 1e-8):
        self.precision: float = precision
        self.session: ddb.session = session
        self.tradeDetails: pd.DataFrame = None

    def getData(self):
        """获取数据"""
        self.tradeDetails = self.session.run(f"""tradeDetails""")
        self.tradeDetails["tradeTime"] = self.tradeDetails["tradeTime"].apply(pd.Timestamp)

    def restore(self) -> List[Dict[pd.Timestamp, Dict[str, float]]]:
        """还原订单 -> 统计每日每个品种的当日pnl & 累计pnl"""
        self.tradeDetails["vol"] = self.tradeDetails["vol"].abs() * self.tradeDetails["state"].map({"open":1, "close":-1})
        longVolDict: Dict[str, int] = {}    # 记录当前每个品种的多单订单剩余量
        shortVolDict: Dict[str, int] = {}   # 记录当前每个品种的空单订单剩余量
        currentPnl: Dict[pd.Timestamp, Dict[str, float]] = {}   # 当前pnl
        cumulativePnl: Dict[pd.Timestamp, Dict[str, float]] = {} # 累计pnl
        lastTradeTime: pd.Timestamp = pd.Timestamp("1900-01-01")
        for _, row in tqdm.tqdm(self.tradeDetails.iterrows(), desc="restoring...", total=self.tradeDetails.shape[0]):
            # 1.时间判断
            tradeTime = pd.Timestamp(row["tradeTime"])
            if lastTradeTime != tradeTime:  # 说明进入到了下一个时间戳
                currentPnl[tradeTime] = {}
                cumulativePnl[tradeTime] = {}
                lastTradeTime = copy.copy(tradeTime)    # 这里必须要拷贝!
            symbol, vol, direction = row["symbol"], row["vol"], row["direction"]

            # 2.更新volDict
            if symbol not in volDict and state == "open":   # 说明当前该品种第一次开仓
                if direction == "long":
                    longVolDict[symbol] = vol
                else:
                    shortVolDict[symbol] = vol
            else:
                if direction == "long" and symbol not in longVolDict and state == "close":
                    raise ValueError(f"{tradeTime}-{symbol}-{direction}: 该品种缺少缺少开仓交易记录")
                if direction == "short" and symbol not in shortVolDict and state == "close":
                    raise ValueError(f"{tradeTime}-{symbol}-{direction}: 该品种缺少缺少开仓交易记录")
                if direction == "long" and symbol in longVolDict:
                    longVolDict[symbol] += vol