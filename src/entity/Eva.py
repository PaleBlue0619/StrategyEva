import pandas as pd
from src.entity.Simulator import Simulator
from typing import Dict, List

class Eva(Simulator):
    def __init__(self, session: ddb.session, statsCfg: Dict[str, any], orderCfg: Dict[str, any], tradeCfg: Dict[str, any]):
        super(Eva, self).__init__(session)
        self.statsCfg: Dict[str, any] = statsCfg
        self.orderCfg: Dict[str, any] = orderCfg
        self.tradeCfg: Dict[str, any] = tradeCfg

    # 现在数据库有statistics & tradeDetails & orderDetails 三张共享内存表

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
