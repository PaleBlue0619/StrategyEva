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
        self.priceDict: Dict[str, float] = {}   # 成交价格-分时
        self.longPos: Dict[str, Dict[str, any]] = {}
        self.shortPos: Dict[str, Dict[str, any]] = {}
        self.longPnl: Dict[str, float] = {}     # 多头Pnl-分时
        self.longMargin: Dict[str, float] = {}  # 多头保证金-分时
        self.shortPnl: Dict[str, float] = {}    # 空头Pnl-分时
        self.shortMargin: Dict[str, float] = {} # 空头保证金-分时
        self.longComm: Dict[str, float] = {}    # 多头手续费-分时
        self.shortComm: Dict[str, float] = {}   # 空头手续费-分时
        self.pnlDF: pd.DataFrame = pd.DataFrame({"tradeTime": [], "symbol": [],
                                                    "longPnl": [], "shortPnl": [], "totalPnl":[],
                                                    "longMargin": [], "shortMargin": [], "totalMargin": [],
                                                    "longComm": [], "shortComm": [], "totalComm":[],
                                                    "pnlRate": [], "commRate": []})   # 当前时间戳的pnl & comm 统计
        self.posDF: pd.DataFrame = pd.DataFrame({"tradeTime": [], "symbol":[],
                                                 "price": [], "longVol": [], "shortVol": [], "totalVol": [],
                                                 "longPosVal": [], "shortPosVal": [], "totalPosVal": []})
    def openPos(self, timestamp: pd.Timestamp, symbol: str, direction: str, price: float, vol: int,
                margin: float, comm: float) -> None:
        """开仓/加仓"""
        self.priceDict[symbol] = price  # 更新价格
        self.currentTime = timestamp    # 更新时间
        if self.currentTime != self.lastTime:
            self.recordPnl()    # 先记录
            self.recordPos()    # 先记录
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
        self.priceDict[symbol] = price  # 更新价格
        self.currentTime = timestamp    # 更新时间
        if self.currentTime != self.lastTime:
            self.recordPnl()    # 先记录
            self.recordPos()    # 先记录
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
            if symbol not in self.longPnl:
                self.longPnl[symbol] = 0.0
            if symbol not in self.longMargin:
                self.longMargin[symbol] = 0.0
            if symbol not in self.longComm:
                self.longComm[symbol] = 0.0
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
            if symbol not in self.shortPnl:
                self.shortPnl[symbol] = 0.0
            if symbol not in self.shortMargin:
                self.shortMargin[symbol] = 0.0
            if symbol not in self.shortComm:
                self.shortComm[symbol] = 0.0
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
        self.pnlDF = pd.concat([self.pnlDF, resultDF], ignore_index=True) # 追加写入

    def recordPos(self) -> None:
        """
        输出当前持仓为DataFrame格式并追加
        """
        symbolList: List[str] = sorted(set(list(self.longPos.keys()) + list(self.shortPos.keys())))
        resultDF = pd.DataFrame({
            "tradeTime": [self.lastTime] * len(symbolList),
            "symbol": symbolList
        })
        longVolDict: Dict[str, float] = {}
        shortVolDict: Dict[str, float] = {}
        longPosVal: Dict[str, float] = {}
        shortPosVal: Dict[str, float] = {}
        for symbol in self.longPos:
            longVolDict[symbol] = sum([i["vol"] for i in self.longPos[symbol]])
            longPosVal[symbol] =sum([i["vol"] * i["price"] for i in self.longPos[symbol]])
        for symbol in self.shortPos:
            shortVolDict[symbol] = sum([i["vol"] for i in self.shortPos[symbol]])
            shortPosVal[symbol] =sum([i["vol"] * i["price"] for i in self.shortPos[symbol]])
        resultDF["price"] = resultDF["symbol"].map(self.priceDict)
        resultDF["longVol"] = resultDF["symbol"].map(longVolDict).fillna(0.0)
        resultDF["shortVol"] = resultDF["symbol"].map(shortVolDict).fillna(0.0)
        resultDF["totalVol"] = resultDF["longVol"] + resultDF["shortVol"]
        resultDF["longPosVal"] = resultDF["symbol"].map(longPosVal).fillna(0.0)
        resultDF["shortPosVal"] = resultDF["symbol"].map(shortPosVal).fillna(0.0)
        resultDF["totalPosVal"] = resultDF["longPosVal"] + resultDF["shortPosVal"]
        self.posDF = pd.concat([self.posDF, resultDF], ignore_index=True) # 追加写入

class Simulator(Position):
    def __init__(self, session: ddb.session):
        super(Simulator, self).__init__()
        self.session: ddb.session = session
        self.tradeDetails: pd.DataFrame = pd.DataFrame()

    def restore_(self, hasProfitCol: bool) -> None:
        """还原订单 -> 统计每日每个品种的当日pnl & 累计pnl & 持仓量+市值"""
        self.tradeDetails = self.session.run(f"""select * from tradeDetails""")
        self.tradeDetails["tradeTime"] = self.tradeDetails["tradeTime"].apply(pd.Timestamp)
        # posDetails的统计必须逐行重构pos进行统计
        for _, row in self.tradeDetails.iterrows():
            state = row["state"]
            if state == "open": # 开仓
                self.openPos(timestamp=row["tradeTime"], symbol=row["symbol"], direction=row["direction"],
                              price=row["price"], vol=row["vol"], margin=row["margin"], comm=row["comm"])
            else:
                self.closePos(timestamp=row["tradeTime"], symbol=row["symbol"], direction=row["direction"],
                               price=row["price"], vol=row["vol"], margin=row["margin"], comm=row["comm"])
        self.pnlDF = self.pnlDF.query("totalPnl!=0").reset_index(drop=True)
        if hasProfitCol:
            self.pnlDF = self.session.run(r"""
            update tradeDetails set margin = 0.0 where state == "open"; // 确保margin只被统计一次
            data = select sum(iif(direction == "long", margin, 0)) as longMargin,
                          sum(iif(direction == "short", margin, 0)) as shortMargin,
                          sum(iif(direction == "long", profit, 0)) as longPnl,
                          sum(iif(direction == "short", profit, 0)) as shortPnl,
                          sum(iif(direction == "long", comm, 0)) as longComm,
                          sum(iif(direction == "short", comm, 0)) as shortComm
                          from tradeDetails where state == "close"
                          group by tradeTime, symbol
            update data set totalMargin = longMargin + shortMargin
            update data set totalPnl = longPnl + shortPnl
            update data set totalComm = longComm + shortComm
            update data set pnlRate = nullFill(totalPnl\totalMargin, 0.0)
            update data set commRate = nullFill(totalComm\totalMargin, 0.0)
            data 
            """)
