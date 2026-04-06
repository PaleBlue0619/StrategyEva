import dolphindb as ddb
import pandas as pd
from src.entity.Result import Result
from src.entity.Simulator import Simulator
from typing import Dict, List

class Eva(Result):
    def __init__(self, session: ddb.session, statistics: pd.DataFrame,
                 tradeDetails: pd.DataFrame, orderDetails: pd.DataFrame,
                 statsCfg: Dict[str, any], orderCfg: Dict[str, any], tradeCfg: Dict[str, any]):
        super(Eva, self).__init__(session, statsCfg=statsCfg, orderCfg=orderCfg, tradeCfg=tradeCfg,
                                  statistics=statistics, tradeDetails=tradeDetails, orderDetails=orderDetails)
        self.statsCfg: Dict[str, any] = statsCfg
        self.orderCfg: Dict[str, any] = orderCfg
        self.tradeCfg: Dict[str, any] = tradeCfg
        self.tradeDetails: pd.DataFrame = tradeDetails
        self.orderDetails: pd.DataFrame = orderDetails
        self.statistics: pd.DataFrame = statistics

    # 现在数据库有statistics & tradeDetails & orderDetails 三张共享内存表
    def restore(self):
        self._restore_()

    def indicatorStats(self) -> Dict[str, float]:
        """Part-I: 策略性能指标统计"""
        resultDict = self.session.run(f"""
        /* 策略性能指标统计 */
        resultDict = dict(STRING, ANY);
        resultDict["totalDays"] = exec count(*) from statistics;
        resultDict["orderDays"] = exec count(*) from (select count(*) from orderDetails group by orderTime)
        resultDict["tradeDays"] = exec count(*) from (select count(*) from tradeDetails group by tradeTime)
        
        """)
        return resultDict
