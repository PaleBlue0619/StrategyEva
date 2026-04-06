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
        self.longPnl: Dict[str, float] = {}     # 多头Pnl-分时
        self.longMargin: Dict[str, float] = {}  # 多头保证金-分时
        self.shortPnl: Dict[str, float] = {}    # 空头Pnl-分时
        self.shortMargin: Dict[str, float] = {} # 空头保证金-分时
        self.longComm: Dict[str, float] = {}    # 多头手续费-分时
        self.shortComm: Dict[str, float] = {}   # 空头手续费-分时
        self.resultDF: pd.DataFrame = pd.DataFrame({"tradeTime": [], "symbol": [],
                                                    "longPnl": [], "shortPnl": [], "totalPnl":[],
                                                    "longMargin": [], "shortMargin": [], "totalMargin": [],
                                                    "longComm": [], "shortComm": [], "totalComm":[],
                                                    "pnlRate": [], "commRate": []})   # 当前时间戳的pnl & comm 统计

    def openPos(self, timestamp: pd.Timestamp, symbol: str, direction: str, price: float, vol: int,
                margin: float, comm: float) -> None:
        """开仓/加仓"""
        self.currentTime = timestamp    # 更新时间
        if self.currentTime != self.lastTime:
            self.recordPnl()    # 先记录
            self.lastTime = copy.copy(timestamp)
        pos = {"vol": vol, "price": price, "margin": margin}
        if direction == "long":
            if symbol not in self.longPos:
                self.longPos[symbol] = [pos]
            else:
                self.longPos[symbol].append(pos)
            if symbol not in self.longPnl:
                self.longPnl[symbol] = 0.0
            if symbol not in self.longMargin:
                self.longMargin[symbol] = 0.0
            if symbol not in self.longComm:
                self.longComm[symbol] = 0.0
            self.longMargin[symbol] += margin
            self.longComm[symbol] += comm
        else:
            if symbol not in self.shortPos:
                self.shortPos[symbol] = [pos]
            else:
                self.shortPos[symbol].append(pos)
            if symbol not in self.shortPnl:
                self.shortPnl[symbol] = 0.0
            if symbol not in self.shortMargin:
                self.shortMargin[symbol] = 0.0
            if symbol not in self.shortComm:
                self.shortComm[symbol] = 0.0
            self.shortMargin[symbol] += margin
            self.shortComm[symbol] += comm

    def closePos(self, timestamp: pd.Timestamp, symbol: str, direction: str, price: float, vol: int,
                 margin: float, comm: float) -> None:
        """平仓"""
        self.currentTime = timestamp    # 更新时间
        if self.currentTime != self.lastTime:
            self.recordPnl()    # 先记录
            self.lastTime = copy.copy(timestamp)
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
            self.longMargin[symbol] = margin
            self.longComm[symbol] += comm
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
            self.shortPnl[symbol] += pnl
            self.shortMargin[symbol] = margin
            self.shortComm[symbol] += comm

    def recordPnl(self) -> None:
        """
        输出当前pnl为DataFrame格式并追加
        Attention: pnl置0合理, 但margin不能置0
        """
        symbolList: List[str] = sorted(set(list(self.longPnl.keys()) + list(self.shortPnl.keys())))
        resultDF = pd.DataFrame({
            "tradeTime": [self.lastTime] * len(symbolList),
            "symbol": symbolList,
        })
        resultDF["longPnl"] = resultDF["symbol"].map(self.longPnl).fillna(0.0)
        resultDF["shortPnl"] = resultDF["symbol"].map(self.shortPnl).fillna(0.0)
        resultDF["totalPnl"] = resultDF["longPnl"] + resultDF["shortPnl"]
        resultDF["longMargin"] = resultDF["symbol"].map(self.longMargin).fillna(0.0)
        resultDF["shortMargin"] = resultDF["symbol"].map(self.shortMargin).fillna(0.0)
        resultDF["totalMargin"] = resultDF["longMargin"] + resultDF["shortMargin"]
        resultDF["longComm"] = resultDF["symbol"].map(self.longComm).fillna(0.0)
        resultDF["shortComm"] = resultDF["symbol"].map(self.shortComm).fillna(0.0)
        resultDF["totalComm"] = resultDF["longComm"] + resultDF["shortComm"]
        resultDF["pnlRate"] = (resultDF["totalPnl"] / resultDF["totalMargin"]).fillna(0.0)
        resultDF["commRate"] = (resultDF["totalComm"] / resultDF["totalMargin"]).fillna(0.0)
        self.resultDF = pd.concat([self.resultDF, resultDF], ignore_index=True) # 追加写入
        # 当前状态置0
        self.longPnl = {i: 0 for i in self.longPnl.keys()}
        self.shortPnl = {i: 0 for i in self.shortPnl.keys()}
        self.longMargin = {i: 0 for i in self.longMargin.keys()}
        self.shortMargin = {i: 0 for i in self.shortMargin.keys()}
        self.longComm = {i: 0 for i in self.longComm.keys()}
        self.shortComm = {i: 0 for i in self.shortComm.keys()}

class Simulator(Position):
    def __init__(self, session: ddb.session, precision: float = 1e-8):
        super(Simulator, self).__init__()
        self.precision: float = precision
        self.session: ddb.session = session
        self.tradeDetails: pd.DataFrame = pd.DataFrame()

    def getData(self):
        """获取数据"""
        self.tradeDetails = self.session.run(f"""tradeDetails""")
        self.tradeDetails["tradeTime"] = self.tradeDetails["tradeTime"].apply(pd.Timestamp)

    def restore(self) -> None:
        """还原订单 -> 统计每日每个品种的当日pnl & 累计pnl"""
        for _, row in self.tradeDetails.iterrows():
            state = row["state"]
            if state == "open": # 开仓
                self.openPos(timestamp=row["tradeTime"], symbol=row["symbol"], direction=row["direction"],
                          price=row["price"], vol=row["vol"], margin=row["margin"], comm=row["comm"])
            else:
                self.closePos(timestamp=row["tradeTime"], symbol=row["symbol"], direction=row["direction"],
                           price=row["price"], vol=row["vol"], margin=row["margin"], comm=row["comm"])
