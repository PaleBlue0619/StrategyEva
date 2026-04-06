import pandas as pd
from src.entity.Simulator import Simulator

class Eva(Simulator):
    def __init__(self, session: ddb.session):
        super(Eva, self).__init__(session)

    # 现在数据库有statistics & tradeDetails & orderDetails 三张共享内存表

    def indicatorStats(self) -> Dict[str, float]:
        """Part-I: 策略性能指标统计"""
        resultDict = self.session.run(f"""
        /* 策略性能指标统计 */
        
        """)
        return resultDict
