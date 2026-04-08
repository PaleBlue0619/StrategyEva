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
        """
        Part-I: 策略性能指标统计
        """
        resultDict = self.session.run(rf"""
        /* 配置项 */
        runStock = false;
        runFuture = true;
        yearDays = 252;
        monthDays = 22;
        basicYearlyRet = 0.028; // 无风险利率
        basicDailyRet = power((1+basicYearlyRet),(1.0\yearDays)) - 1 
        if (runFuture == 0 and runStock == 1){{
            statistics_ = select * from statistics where stockProfit!=0
            update statistics_ set profitDiff = deltas(stockProfit)
            update statistics_ set dailyRet = nullFill(profitDiff\prev(stockProfit), 0.0)
        }}else if(runFuture == 1 and runStock == 0){{
            statistics_ = select * from statistics where futureProfit!=0 
            update statistics_ set profitDiff = deltas(futureProfit)
            update statistics_ set dailyRet = nullFill(profitDiff\prev(futureProfit), 0.0)
        }}else{{
            statistics_ = select * from statistics where profit!=0
            update statistics set profitDiff = deltas(profit)
            update statistics_ set dailyRet = nullFill(profitDiff\prev(profit), 0.0)
        }}
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
        resultDict["drawDownDF"] = drawDownDF;
        resultDict
        """)
        return resultDict

    def summaryPnlStats(self) -> Dict[str, pd.DataFrame]:
        """
        PartII-A: 账户整体Pnl分析
        """
        resultDict = self.session.run("""
        // 账户pnl分析
        resultDict = dict(STRING, ANY)
        weekPnlSeries = select (last(profit)-first(profit))-(last(comm)-first(comm)) as pnl,last(profit)-first(profit) as profit, last(comm)-first(comm) as comm from statistics group by year(tradeDate) as year,weekOfYear(tradeDate) as week
        update weekPnlSeries set cumPnl = cumsum(pnl)
        update weekPnlSeries set cumProfit = cumsum(profit)
        update weekPnlSeries set cumComm = cumsum(comm)
        monthPnlSeries = select (last(profit)-first(profit))-(last(comm)-first(comm)) as pnl,last(profit)-first(profit) as profit, last(comm)-first(comm) as comm from statistics group by year(tradeDate) as year,monthOfYear(tradeDate) as month
        update monthPnlSeries set cumPnl = cumsum(pnl)
        update monthPnlSeries set cumProfit = cumsum(profit)
        update monthPnlSeries set cumComm = cumsum(comm)
        yearPnlSeries = select (last(profit)-first(profit))-(last(comm)-first(comm)) as pnl,last(profit)-first(profit) as profit, last(comm)-first(comm) as comm from statistics group by year(tradeDate) as year
        update yearPnlSeries set cumPnl = cumsum(pnl)
        update yearPnlSeries set cumProfit = cumsum(profit)
        update yearPnlSeries set cumComm = cumsum(comm)
        resultDict["weekPnl"] = weekPnlSeries;
        resultDict["monthPnl"] = monthPnlSeries;
        resultDict["yearPnl"] = yearPnlSeries;
        resultDict;
        """)
        return resultDict

    def pnlStatsBySymbol(self, symbol: str) -> pd.DataFrame:
        """
        PartII-B: 分品种Pnl分析
        """
        resultDF = self.session.run(f"""
        symbolStr = "{symbol}"
        pt = select tradeTime, pnlRate, commRate, longPnl-longComm as longPnl, shortPnl-shortComm as shortPnl, totalPnl-totalComm as totalPnl from pnlDetails where symbol == symbolStr
        update pt set cumLongPnl = cumsum(longPnl)
        update pt set cumShortPnl = cumsum(shortPnl)
        update pt set cumTotalPnl = cumsum(totalPnl)
        pt
        """)
        return resultDF

    def pnlStatsByPeriod(self, startDate: pd.Timestamp, endDate: pd.Timestamp) -> pd.DataFrame:
        """
        PartII-C: 给定起始时间的品种Pnl+排名
        """
        startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        resultDF = self.session.run(rf"""
        /* 配置项 */
        startDate = {startDate};
        endDate = {endDate};
        pt = select sum(longMargin) as longMargin, sum(shortMargin) as shortMargin, sum(totalMargin) as totalMargin, 
                    sum(longComm) as longComm, sum(shortComm) as shortComm, sum(totalComm) as totalComm, 
                    sum(longPnl) as longPnl, sum(shortPnl) as shortPnl, sum(totalPnl) as totalPnl 
                    from pnlDetails where tradeTime between startDate and endDate group by symbol
        resultDF = select *, nullFill((longPnl-longComm)\longMargin,0.0) as longPnlRate, 
                            nullFill((shortPnl-shortComm)\shortMargin,0.0) as shortPnlRate, 
                            nullFill((totalPnl-totalComm)\totalMargin,0.0) as totalPnlRate from pt
        undef(`pt);
        resultDF;
        """)
        return resultDF

    def summaryTradeStats(self) -> Dict[str, pd.DataFrame]:
        """PartIII-A: 交易行为分析
        including:
        · 策略累计交易次数(开仓/平仓/总)
        · 策略时序换手率(每日/每周/每月/每年)
        """
        resultDict = self.session.run(rf"""
        /* 策略累计交易次数(开仓/平仓/总) */
        resultDict = dict(STRING, ANY);
        resultDF = select count(*) as totalTradeNum, sum(iif(state=="open", 1, 0)) as openTradeNum, sum(iif(state=="close", 1, 0)) as closeTradeNum, sum(iif(direction=="long", 1, 0)) as longTradeNum, sum(iif(direction=="short", 1, 0)) as shortTradeNum from tradeDetails group by date(tradeTime) as tradeDate
        dateList = resultDF[`tradeDate]
        dropColumns!(resultDF,`tradeDate)
        resultDF = select dateList as tradeDate, * from cumsum(resultDF)
        resultDict["tradeNumStats"] = resultDF
        
        /* 策略时序换手率(每日/每周/每月/每年) 
        换手率 = (卖出旧仓位总额 + 买入新仓位总额)\(平均总市值)
        */
        dailyDF = select sum(iif(state=="open", price*vol, 0.0)) as openVal,
                  sum(iif(state=="close", price*vol, 0.0)) as closeVal
            from tradeDetails group by date(tradeTime) as tradeDate
        posValDF = select sum(totalPosVal) as totalPosVal from posDetails group by date(tradeTime) as tradeDate 
        dailyDF = lj(dailyDF, posValDF, `tradeDate)
        update dailyDF set totalPosVal = NULL where totalPosVal == 0
        update dailyDF set totalPosVal = totalPosVal.ffill(); // 持仓市值向后填充
        update dailyDF set turnoverRate = nullFill((openVal+closeVal)\totalPosVal,0.0)
        weekDF = select sum(openVal) as openVal, sum(closeVal) as closeVal, avg(totalPosVal) as totalPosVal
                from dailyDF group by year(tradeDate) as year, weekOfYear(tradeDate) as week
        monthDF = select sum(openVal) as openVal, sum(closeVal) as closeVal, avg(totalPosVal) as totalPosVal
                  from dailyDF group by year(tradeDate) as year, monthOfYear(tradeDate) as month
        yearDF = select sum(openVal) as openVal, sum(closeVal) as closeVal, avg(totalPosVal) as totalPosVal
                  from dailyDF group by year(tradeDate) as year
        resultDict["dailyTurnoverRateStats"] = select tradeDate, nullFill((openVal+closeVal)\totalPosVal,0.0) as turnoverRate from dailyDF;
        resultDict["weeklyTurnoverRateStats"] = select year, week, nullFill((openVal+closeVal)\totalPosVal,0.0) as turnoverRate from weekDF;
        resultDict["monthlyTurnoverRateStats"] = select year, month, nullFill((openVal+closeVal)\totalPosVal,0.0) as turnoverRate from monthDF;
        resultDict["yearlyTurnoverRateStats"] = select year, nullFill((openVal+closeVal)\totalPosVal,0.0) as turnoverRate from yearDF;
        undef(`weekDF`monthDF`yearDF`dailyDF`posValDF)
        resultDict;
        """)
        return resultDict

    def tradeStatsByPeriod(self, startDate: pd.Timestamp, endDate: pd.Timestamp) -> pd.DataFrame:
        """
        PartIII-B: 分品种止盈止损触发次数/概率 + 排名
        """
        startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        resultDF = self.session.run(rf"""
        startDate = {startDate}
        endDate = {endDate}
        resultDF = select sum(iif(state=="close", 1, 0)) as closeTradeNum, 
                          sum(iif(reason like "static%" or reason like "dynamic%", 1, 0)) as priceLimitNum, 
                          sum(iif((state=="close" and direction == "long" and reason=="staticHigh") or 
                                  (state=="close" and direction == "short" and reason=="staticLow"), 1, 0)) as staticProfitNum, 
                          sum(iif((state=="close" and direction == "short" and reason=="staticHigh") or 
                                  (state=="close" and direction == "long" and reason=="staticLow"), 1, 0)) as staticLoseNum
                   from tradeDetails where date(tradeTime) between startDate and endDate group by symbol
        update resultDF set staticProfitLimitRate = nullFill(staticProfitNum\priceLimitNum, 0.0) 
        update resultDF set staticLoseLimitRate = nullFill(staticLoseNum\priceLimitNum, 0.0) 
        resultDF
        """)
        return resultDF