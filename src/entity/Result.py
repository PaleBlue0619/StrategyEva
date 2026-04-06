import pandas as pd
import dolphindb as ddb
import streamlit as st
from functools import lru_cache
from typing import Dict, List
from src.entity.Table import Statistics, TradeDetails, OrderDetails

class Result(Statistics, TradeDetails, OrderDetails):
    def __init__(self, session: ddb.session, statistics: pd.DataFrame,
                 tradeDetails: pd.DataFrame, orderDetails: pd.DataFrame):
        super().__init__(session, pd.DataFrame())
        self.statistics: pd.DataFrame = statistics
        self.tradeDetails: pd.DataFrame = tradeDetails
        self.orderDetails: pd.DataFrame = orderDetails

    def upload(self, statsCfg: Dict[str, any], orderCfg: Dict[str, any], tradeCfg: Dict[str, any]):
        S = Statistics(session=self.session, data=self.statistics)
        S.fromDict(statsCfg)
        S.initTable()
        S.upload_()
        O = OrderDetails(session=self.session, data=self.orderDetails)
        O.initTable()
        O.fromDict(orderCfg)
        O.upload_()
        T = TradeDetails(session=self.session, data=self.tradeDetails)
        T.fromDict(tradeCfg)
        T.initTable()
        T.upload_()