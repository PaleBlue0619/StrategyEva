import dolphindb as ddb
import pandas as pd
from typing import Dict, List
from src.entity.Simulator import Simulator

class Table:
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        self.tableName: str = "tbName"
        self.data: pd.DataFrame = data
        self.session: ddb.session = session

    def initTable(self):
        pass

    def upload_(self):
        self.session.upload({self.tableName: self.data})

class Statistics(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, data)
        self.tableName: str = "statistics"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    def initTable(self):
        """初始化共享内存表"""
        colNames = ["tradeDate", "cash", "comm",
                    "stockCash", "stockComm", "futureCash", "futureComm",
                    "profit", "stockProfit", "futureProfit",
                    "realTimeProfit", "stockRealTimeProfit", "futureRealTimeProfit"]
        colTypes = ["DATE", "DOUBLE", "DOUBLE",
                    "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE",
                    "DOUBLE", "DOUBLE", "DOUBLE",
                    "DOUBLE", "DOUBLE", "DOUBLE"]
        self.session.upload({"colNames": colNames, "colTypes": colTypes})
        self.session.run(f"""
        try{{undef("{self.tableName}", SHARED)}}catch(ex){{}}; // 先删除共享表
        tab = table(1:0, colNames, colTypes); 
        share(tab, "{self.tableName}"); // 创建共享内存表
        """)

    def upload_(self):   # 上传 + 改名
        colDict: Dict[str, str] = {self.cfg["indicator"][i]: str(i).replace("Col", "") for i in self.cfg["indicator"]}
        self.data.rename(columns=colDict, inplace=True)
        self.data = self.data[list(colDict.values())]
        self.session.upload({"tab": self.data})
        self.session.run(f"""objByName("{self.tableName}",true).append!(tab); undef(`tab);""")

class OrderDetails(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, data)
        self.tableName: str = "orderDetails"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    def initTable(self):
        """初始化共享内存表"""
        colNames = ["orderNum", "orderTime", "symbol", "direction", "state", "price", "vol", "reason"]
        colTypes = ["INT", "DATE", "SYMBOL", "STRING", "STRING", "DOUBLE", "DOUBLE", "STRING"]
        self.session.upload({"colNames": colNames, "colTypes": colTypes})
        self.session.run(f"""
        try{{undef("{self.tableName}", SHARED)}}catch(ex){{}}; // 先删除共享表
        tab = table(1:0, colNames, colTypes); 
        share(tab, "{self.tableName}"); // 创建共享内存表
        """)

    def upload_(self):   # 规范状态名称 + dmlStr + 上传 + 改名
        colDict: Dict[str, str] = {self.cfg["indicator"][i]: str(i).replace("Col", "") for i in self.cfg["indicator"]}
        reasonCol = self.cfg["indicator"]["reasonCol"]
        self.data[reasonCol] = self.data[reasonCol].map({j: i for i, j in self.cfg["reasonState"].items()})
        self.data.rename(columns=colDict, inplace=True)
        self.data = self.data[list(colDict.values())]
        self.session.upload({"tab": self.data})
        self.session.run(f"""objByName("{self.tableName}",true).append!(tab); undef(`tab);""")
        if self.cfg["dmlStr"] not in ["", None]:
            self.session.run(self.cfg["dmlStr"])

class TradeDetails(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, data)
        self.tableName: str = "tradeDetails"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    def initTable(self):
        """初始化共享内存表"""
        colNames = ["tradeNum", "tradeTime", "symbol", "direction", "state", "price", "vol", "margin", "profit", "comm", "reason"]
        colTypes = ["INT", "DATE", "SYMBOL", "STRING", "STRING", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "STRING"]
        self.session.upload({"colNames": colNames, "colTypes": colTypes})
        self.session.run(f"""
        try{{undef("{self.tableName}", SHARED)}}catch(ex){{}}; // 先删除共享表
        tab = table(1:0, colNames, colTypes); 
        share(tab, "{self.tableName}"); // 创建共享内存表
        """)

    def upload_(self):   # 规范状态名称 + dmlStr + 上传 + 改名
        colDict: Dict[str, str] = {self.cfg["indicator"][i]: str(i).replace("Col", "") for i in self.cfg["indicator"]}
        reasonCol = self.cfg["indicator"]["reasonCol"]
        self.data[reasonCol] = self.data[reasonCol].map({j: i for i, j in self.cfg["reasonState"].items()})
        self.data.rename(columns=colDict, inplace=True)
        self.data = self.data[list(colDict.values())]
        self.session.upload({"tab": self.data})
        self.session.run(f"""objByName("{self.tableName}",true).append!(tab); undef(`tab);""")
        if self.cfg["dmlStr"] not in ["", None]:
            self.session.run(self.cfg["dmlStr"])

class OtherDetails(Table, Simulator):
    # PnlDetails & PosDetails
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        Table.__init__(self, session, data)
        Simulator.__init__(self, session)
        self.tableCfg: Dict[str, any] = {
            "posDetails": {
                "tableName": "posDetails",
                "colNames": ["tradeTime", "symbol", "price", "longVol", "shortVol", "totalVol", "longPosVal", "shortPosVal", "totalPosVal"],  # 由于缺乏行情数据, 这里的posVal是按照oriPrice计算的持仓价值
                "colTypes": ["DATE", "SYMBOL", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE"]
            },
            "pnlDetails": {
                "tableName": "pnlDetails",
                "colNames": ["tradeTime", "symbol", "longPnl", "shortPnl", "totalPnl","longMargin", "shortMargin", "totalMargin","longComm", "shortComm", "totalComm","pnlRate", "commRate"],
                "colTypes": ["DATE", "SYMBOL", "DOUBLE", "DOUBLE", "DOUBLE","DOUBLE", "DOUBLE", "DOUBLE","DOUBLE", "DOUBLE", "DOUBLE","DOUBLE", "DOUBLE"]
            }
        }

    def initTable(self):
        """初始化共享内存表"""
        for tableName in self.tableCfg:
            cfg = self.tableCfg[tableName]
            self.session.upload({"colNames": cfg["colNames"], "colTypes": cfg["colTypes"]})
            self.session.run(f"""
            try{{undef("{tableName}", SHARED)}}catch(ex){{}}; // 先删除共享表
            tab = table(1:0, colNames, colTypes); 
            share(tab, "{tableName}"); // 创建共享内存表
            """)

    def upload_(self):
        self.posDF = self.posDF[self.tableCfg["posDetails"]["colNames"]]    # 调整列顺序
        self.session.upload({"tab": self.posDF})
        self.session.run(f"""objByName("{self.tableCfg["posDetails"]["tableName"]}",true).append!(tab); undef(`tab);""")
        self.pnlDF = self.pnlDF[self.tableCfg["pnlDetails"]["colNames"]]    # 调整列顺序
        self.session.upload({"tab": self.pnlDF})
        self.session.run(f"""objByName("{self.tableCfg["pnlDetails"]["tableName"]}",true).append!(tab); undef(`tab);""")