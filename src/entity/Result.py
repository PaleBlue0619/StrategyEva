import pandas as pd
import dolphindb as ddb
import streamlit as st
from functools import lru_cache
from typing import Dict, List
from src.entity.Table import Statistics, TradeDetails, OrderDetails, PnlDetails
from src.entity.Simulator import Simulator

class Result(Statistics, TradeDetails, OrderDetails, PnlDetails, Simulator):
    def __init__(self, session: ddb.session, statistics: pd.DataFrame,
                 tradeDetails: pd.DataFrame, orderDetails: pd.DataFrame,
                 statsCfg: Dict[str, any], orderCfg: Dict[str, any], tradeCfg: Dict[str, any]):
        Statistics.__init__(self, session, statistics)
        TradeDetails.__init__(self, session, tradeDetails)
        OrderDetails.__init__(self, session, orderDetails)
        PnlDetails.__init__(self, session, pd.DataFrame())
        Simulator.__init__(self, session)
        self.statistics: pd.DataFrame = statistics
        self.tradeDetails: pd.DataFrame = tradeDetails
        self.orderDetails: pd.DataFrame = orderDetails
        self.statsCfg: Dict[str, any] = statsCfg
        self.orderCfg: Dict[str, any] = orderCfg
        self.tradeCfg: Dict[str, any] = tradeCfg

    def upload(self):
        S = Statistics(session=self.session, data=self.statistics)
        S.fromDict(self.statsCfg)
        S.initTable()
        S.upload_()
        O = OrderDetails(session=self.session, data=self.orderDetails)
        O.initTable()
        O.fromDict(self.orderCfg)
        O.upload_()
        T = TradeDetails(session=self.session, data=self.tradeDetails)
        T.fromDict(self.tradeCfg)
        T.initTable()
        T.upload_()

    def _restore_(self):
        hasProfitCol: bool = True if self.tradeCfg["indicator"]["profitCol"] else False
        P = PnlDetails(session=self.session, data=self.tradeDetails)
        P.initTable()
        P.restore_(hasProfitCol=hasProfitCol)
        P.upload_()

    def getSymbolList(self, scale: str) -> List[str]:
        """获取所有标的列表(order/trade)"""
        if scale == "order":
            return self.session.run(f"""exec distinct(symbol) from orderDetails""")
        elif scale == "trade":
            return self.session.run(f"""exec distinct(symbol) from tradeDetails""")
        else:
            return sorted(set(self.getSymbolList(scale="order") + self.getSymbolList(scale="trade")))

    def getDateList(self, scale: str) -> List[pd.Timestamp]:
        """获取所有时间列表(order/trade/stats)"""
        if scale == "order":
            return self.session.run(f"""exec distinct(date(orderTime)) from orderDetails""")
        elif scale == "trade":
            return self.session.run(f"""exec distinct(date(tradeTime)) from tradeDetails""")
        elif scale in ["statistics", "stats"]:
            return self.session.run(f"""exec distinct(date(tradeDate)) from statistics""")
        else:
            return sorted(set(self.getDateList(scale="order") + self.getDateList(scale="trade") + self.getDateList(scale="stats")))
