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
        resultDict = self.session.run(r"""
        /* 配置项 */
        runStock = false;
        runFuture = true;
        yearDays = 252;
        monthDays = 22;
        basicYealyRet = 0.028; // 无风险利率
        basicDailyRet = power((1+basicYealyRet),(1.0\yearDays)) - 1 
        if (runFuture == false and runStock == true){
            statistics_ = select * from statistics where stockProfit!=0
            update statistics_ set profitDiff = deltas(stockProfit)
            update statistics_ set dailyRet = nullFill(profitDiff\prev(stockProfit), 0.0)
        }else if(runFuture == true and runStock == false){
            statistics_ = select * from statistics where futureProfit!=0 
            update statistics_ set profitDiff = deltas(futureProfit)
            update statistics_ set dailyRet = nullFill(profitDiff\prev(futureProfit), 0.0)
        }else{
            statistics_ = select * from statistics where profit!=0
            update statistics set profitDiff = deltas(profit)
            update statistics_ set dailyRet = nullFill(profitDiff\prev(profit), 0.0)
        }
        tradeDetails_ = select * from tradeDetails where state="close"
        update statistics set netValue = nullFill((profit-comm)\first(cash),0.0)+1.0; // 计算净值
        
        /* 策略性能指标统计 */
        resultDict = dict(STRING, ANY);
        // 简单指标
        resultDict["totalDays"] = exec count(*) from statistics;
        resultDict["orderDays"] = exec count(*) from (select count(*) from orderDetails group by orderTime);
        resultDict["tradeDays"] = exec count(*) from (select count(*) from tradeDetails_ group by tradeTime);
        resultDict["winDays"] = exec count(*) from statistics_ where profitDiff > 0
        resultDict["loseDays"] = exec count(*) from statistics_ where profitDiff < 0
        resultDict["winDaysRate"] = nullFill(resultDict["winDays"]\resultDict["tradeDays"], 0.0)
        resultDict["loseDaysRate"] =  nullFill(resultDict["loseDays"]\resultDict["tradeDays"], 0.0)
        
        resultDict["orderNum"] = exec count(*) from orderDetails 
        resultDict["longOrderNum"] = exec count(*) from orderDetails where direction == "long"
        resultDict["shortOrderNum"] = exec count(*) from orderDetails where direction == "short"
        resultDict["tradeNum"] = exec count(*) from pnlDetails
        resultDict["openTradeNum"] = exec count(*) from tradeDetails where state == "open"
        resultDict["closeTradeNum"] = exec count(*) from tradeDetails where state == "close"
        resultDict["longTradeNum"] = exec count(*) from tradeDetails where direction == "long"
        resultDict["shortTradeNum"] = exec count(*) from tradeDetails where direction == "short"
        resultDict["winTrade"] = exec count(*) from pnlDetails where totalPnl>0
        resultDict["winTradeRate"] = nullFill(resultDict["winTrade"]\resultDict["tradeNum"], 0.0)
        resultDict["sysTradeNum"] = (exec count(*) from tradeDetails) - resultDict["orderNum"]
        resultDict["sysTradeRate"] = nullFill(resultDict["sysTradeNum"]\resultDict["tradeNum"], 0.0)
        
        // 复杂指标
        resultDict["periodRet"] = exec (last(profit)-last(comm))\firstNot(cash) from statistics
        resultDict["dailyRet"] = power(1 + resultDict["periodRet"], 1\resultDict["totalDays"])- 1
        resultDict["weeklyRet"] = power(1 + resultDict["periodRet"], 5\resultDict["totalDays"])- 1
        resultDict["monthlyRet"] = power(1 + resultDict["periodRet"], monthDays\resultDict["totalDays"])- 1
        resultDict["yearlyRet"] = power(1 + resultDict["periodRet"], yearDays\resultDict["totalDays"])- 1
        resultDict["maxUpStreakDays"] = exec maxPositiveStreak(iif(profitDiff>=0, 1, 0)) from statistics_ // 最长连涨天数
        resultDict["maxDownStreakDays"] = exec maxPositiveStreak(iif(profitDiff<=0, 1, 0)) from statistics_ // 最长连跌天数
        resultDict["maxZeroStreakDays"] = exec maxPositiveStreak(iif(profitDiff==0, 1, 0)) from statistics_// 最长不变天数
        resultDict["sharpeRatio"] = (mean(statistics_["dailyRet"])-basicDailyRet)\stdp(statistics_["dailyRet"])
        resultDict["shareRatio1"] = ((exec mean(dailyRet) from statistics_ where dailyRet!=0)-basicDailyRet)\(exec stdp(dailyRet) from statistics_ where dailyRet!=0)
        resultDict["sortinoRatio"] = (mean(statistics_["dailyRet"])-basicDailyRet)\(exec stdp(dailyRet) from statistics_ where dailyRet<0)
        resultDict["sortinoRatio1"] = ((exec mean(dailyRet) from statistics_ where dailyRet!=0)-basicDailyRet)\(exec stdp(dailyRet) from statistics_ where dailyRet<0)
        
        // 回撤指标
        update statistics set peak = cummax(netValue); 
        update statistics set isDrawDown = iif(netValue<peak, 1, 0); //标记是否处于回撤期
        update statistics set currentDrawDown = 0.0
        update statistics set currentDrawDown = (peak-netValue)\peak where isDrawDown == 1
        update statistics set period = 0.0;
        update statistics set period = 1.0 where isDrawDown!=prev(isDrawDown)
        update statistics set cumPeriod = cumsum(period)
        resultDict["maxDrawdown"] = exec max(currentDrawDown) from statistics; // 最大回撤
        drawDownDF = select startDate, endDate, nDays, drawDownRate from (select first(tradeDate) as startDate, last(tradeDate) as endDate, count(*) as nDays, max(currentDrawDown) as drawdownRate from statistics group by cumPeriod) where drawDownRate > 0 order by drawDownRate desc
                
        resultDict
        """)
        return resultDict
