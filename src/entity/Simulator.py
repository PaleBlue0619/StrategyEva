"""
订单还原类
"""
import dolphindb as ddb
import pandas as pd
from typing import Dict, List

class Simulator:
    def __init__(self, session: ddb.session, precision: float = 1e-8):
        self.precision: float = precision
        self.session: ddb.session = session
        self.orderDetails: pd.DataFrame = None
        self.tradeDetails: pd.DataFrame = None

    def getData(self):
        """获取数据"""
        self.tradeDetails = self.session.run(f"""tradeDetails""")
        self.tradeDetails["tradeTime"] = self.tradeDetails["tradeTime"].apply(pd.Timestamp)

    def restore(self) -> List[Dict[pd.Timestamp, Dict[str, float]]]:
        """还原订单 -> 统计每日每个品种的当日pnl & 累计pnl"""
        volDict: Dict[str, int] = {}    # 记录当前每个品种的订单剩余量
        currentPnl: Dict[pd.Timestamp, Dict[str, float]] = {}   # 当前pnl
        cumulativePnl: Dict[pd.Timestamp, Dict[str, float]] = {} # 累计pnl
        lastTradeTime: pd.Timestamp = pd.Timestamp("1900-01-01")
        for _, row in self.orderDetails.iterrows():
            tradeTime = pd.Timestamp(row["tradeTime"])
            if lastTradeTime != tradeTime:  # 说明进入到了下一个时间戳
                currentPnl[tradeTime] = {}
                cumulativePnl[tradeTime] = {}
                lastTradeTime = tradeTime.copy()    # 这里必须要拷贝!

