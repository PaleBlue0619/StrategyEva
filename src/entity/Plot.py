import pandas as pd
import dolphindb as ddb
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from src.entity.Result import Result
from src.entity.Eva import Eva
from typing import List, Dict

class Plot(Eva):
    def __init__(self, session: ddb.session, statistics: pd.DataFrame, tradeDetails: pd.DataFrame,
                 orderDetails: pd.DataFrame, statsCfg: Dict[str, any], orderCfg: Dict[str, any],
                 tradeCfg: Dict[str, any]):
        Result.__init__(self, session, statistics, tradeDetails, orderDetails, statsCfg, orderCfg, tradeCfg)
        super().__init__(session, statistics, tradeDetails, orderDetails, statsCfg, orderCfg, tradeCfg)
        self.indicatorRes: Dict[str, any] = {}
        self.summaryPnlRes: Dict[str, pd.DataFrame] = {}
        self.upload()
        self.restore()
        self.statistics.set_index("tradeDate",inplace=True)

    # @staticmethod
    # def heatMap(data: pd.DataFrame, valueCol: str, idx1Col: str, idx2Col: str, title: str):
    #     # 创建透视表
    #     pivot_table = data.pivot_table(
    #         values=valueCol,
    #         index=idx1Col,
    #         columns=idx2Col,
    #         aggfunc='first',
    #         fill_value=0
    #         )
    #     pivot_table = pivot_table.sort_index()
    #
    #
    #     fig = px.imshow(
    #         pivot_table,
    #         labels=dict(x=f"{idx2Col}", y=f"{idx1Col}", color=valueCol),
    #         x=pivot_table.columns,
    #         y=pivot_table.index,
    #         title=title,
    #         color_continuous_scale='RdYlGn',  # 红-黄-绿渐变
    #         aspect="auto",
    #         text_auto='.0f'  # 显示数值
    #     )
    #
    #     # 优化布局
    #     fig.update_layout(
    #         height=600,
    #         xaxis_title=f"{idx2Col}",
    #         yaxis_title=f"{idx1Col}",
    #         xaxis={'side': 'top'},  # 年份显示在顶部
    #         coloraxis_colorbar=dict(
    #             title=valueCol,
    #             thickness=20,
    #     )
    #     )
    #
    #     # 调整字体大小
    #     fig.update_xaxes(tickangle=0)
    #     return fig
    @staticmethod
    def heatMap(data: pd.DataFrame, valueCol: str, idx1Col: str, idx2Col: str, title: str):
        # 创建透视表
        pivot_table = data.pivot_table(
            values=valueCol,
            index=idx1Col,
            columns=idx2Col,
            aggfunc='first',
            fill_value=0
        )
        pivot_table = pivot_table.sort_index()

        # 获取数据范围，用于对称配色
        max_abs_value = max(abs(pivot_table.min().min()), abs(pivot_table.max().max()))

        # 红涨绿跌：正值(>0)红色，负值(<0)绿色，0白色
        # 颜色刻度: 0=最小值(绿) -> 0.5=0值(白) -> 1=最大值(红)
        custom_colorscale = [
            [0.0, "green"],  # 负值最大 -> 深绿
            [0.5, "white"],  # 0值 -> 白色
            [1.0, "red"]  # 正值最大 -> 红色
        ]

        fig = px.imshow(
            pivot_table,
            labels=dict(x=f"{idx2Col}", y=f"{idx1Col}", color=valueCol),
            x=pivot_table.columns,
            y=pivot_table.index,
            title=title,
            color_continuous_scale=custom_colorscale,
            zmin=-max_abs_value,  # 负的最大值
            zmax=max_abs_value,  # 正的最大值
            aspect="auto",
            text_auto='.0f'
        )

        # 优化布局
        fig.update_layout(
            height=600,
            xaxis_title=f"{idx2Col}",
            yaxis_title=f"{idx1Col}",
            xaxis={'side': 'top'},
            coloraxis_colorbar=dict(
                title=valueCol,
                thickness=20,
                tickvals=[-max_abs_value, 0, max_abs_value],
                ticktext=[f"负值\n{-max_abs_value:.0f}", "0\n白色", f"正值\n{max_abs_value:.0f}"]
            )
        )

        # 调整字体大小
        fig.update_yaxes(dtick=1)
        fig.update_xaxes(tickangle=0)

        return fig

    def indicatorPlot(self):
        """Statistics(左) + Eva指标(右)"""
        if self.indicatorRes == {}:
            self.indicatorRes = self.indicatorStats()
        if self.summaryPnlRes == {}:
            self.summaryPnlRes = self.summaryPnlStats()
        st.set_page_config(layout="wide", page_title="indicatorStats")
        st.title("指标统计")
        leftPart, rightPart = st.columns([7, 3], gap="medium")
        with leftPart:
            st.markdown("**statistics plot**")
            st.line_chart(self.statistics[["cash","profit","realTimeProfit"]], width='stretch')
            st.divider()    # 分隔线, 展示不同图表
            st.markdown("**maxDrawDown stats**")
            st.dataframe(self.indicatorRes["drawDownDF"])
            st.markdown("**pnlByPeriod plot**")
            st.divider()
            data = self.summaryPnlRes["yearPnl"].set_index("year")
            st.bar_chart(data[["pnl","profit","comm"]], stack=False, width='stretch')
            st.area_chart(data[["cumPnl","cumProfit"]])
            data = self.summaryPnlRes["monthPnl"]
            st.plotly_chart(self.heatMap(data, "pnl", idx1Col="year", idx2Col="month", title="pnlByPeriod"), use_container_width=True)

        with rightPart:
            # 使用 st.metric 展示指标卡片
            indicatorDict = {
                "总天数": ["totalDays", "策略运行时包含的交易日数量"],
                "订单天数": ["orderDays", "有订单的天数"],
                "交易天数": ["tradeDays", "有成交的天数(系统交易与用户交易皆可)"],
                "其中:盈利天数": ["winDays", "相比上一个交易日pnlDiff>0的天数"],
                "亏损天数": ["loseDays", "相比上一个交易日pnlDiff<0的天数"],
                "盈利天数占比": ["winDaysRate", "winDays/totalDays"],
                "亏损天数占比": ["loseDaysRate", "loseDays/totalDays"],
                "divider1": ["",""],
                "订单次数": ["orderNum", ""],
                "其中:多单订单次数": ["longOrderNum", "开多+平多的订单次数"],
                "空单订单次数": ["shortOrderNum", "开空+平空的订单次数"],
                "divider2": ["", ""],
                "成交次数": ["tradeNum", ""],
                "其中:开仓次数": ["openTradeNum", ""],
                "平仓次数": ["closeTradeNum", ""],
                # "多单成交次数": ["longTradeNum", "开多+平多的成交次数"],
                # "空单成交次数": ["shortTradeNum", "开空+平空的成交次数"],
                # "成交盈利次数": ["winTrade", "策略运行时盈利的平仓次数"],
                "成交盈利比例": ["winTradeRate", "winTrade/closeTradeNum"],
                "divider3": ["", ""],
                "区间收益率": ["periodRet", ""],
                "日收益率": ["dailyRet", ""],
                "周收益率": ["weeklyRet", ""],
                "月收益率": ["monthlyRet", ""],
                "年化收益率": ["yearlyRet", ""],
                "divider4": ["", ""],
                "最长不跌天数": ["maxUpStreakDays",""],
                "最长不涨天数": ["maxDownStreakDays",""],
                "最长不交易天数": ["maxZeroStreakDays",""],
                "夏普比率": ["sharpeRatio", "年化夏普比率"],
                "夏普比率(剔除不交易时段)": ["sharpeRatio1", "年化夏普比率"],
                "索提诺比率": ["sortinoRatio", "年化索提诺比率"],
                "索提诺比率(剔除不交易时段)": ["sortinoRatio1", "年化索提诺比率"],
            }
            for key, [value, helpStr] in indicatorDict.items():
                if key.startswith("divider"):
                    st.divider()
                    continue
                value = self.indicatorRes[value]
                if key in ["盈利天数占比","亏损天数占比","成交盈利比例",
                           "区间收益率","日收益率","周收益率","月收益率","年化收益率"]:
                    value = f"{value*100:.2f}%"
                if key in ["夏普比率", "夏普比率(剔除不交易时段)", "索提诺比率", "索提诺比率(剔除不交易时段)"]:
                    value = f"{value:.2f}"
                st.metric(
                    label=key,  # 展示的名称
                    value=value, # 后端传来的名称
                    delta=None,
                    help=helpStr
                )
        print(self.statistics)
        print(self.indicatorRes)
        print(self.summaryPnlRes)