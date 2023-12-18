import pymysql
from dbutils.pooled_db import PooledDB


class DatabaseConnector:
    def __init__(self, db_info):
        """
        使用提供的 db_info 初始化 DatabaseConnector 实例。
        """
        self.db_info = db_info
        self.connection = None
        self.pool = PooledDB(
            creator=pymysql,  # 用于连接数据库的模块
            maxconnections=5,  # 连接池允许的最大连接数
            mincached=2,       # 初始化时，连接池中至少创建的空闲的连接，0表示不创建
            maxcached=5,       # 连接池中最多闲置的连接，0和None不限制
            blocking=True,     # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,     # 一个连接最多被重复使用的次数，None表示无限制
            setsession=[],     # 开始会话前执行的命令列表
            ping=0,            # ping MySQL服务端，检查是否服务可用。
            **db_info
        )

    def get_connection(self):
        """
        Connect to the database using the db_info provided during instantiation.
        """
        if self.connection is not None:
            # 已有连接，可以选择重用或关闭后重新连接
            return self.pool.connection()

        try:
            self.connection = pymysql.connect(
                host=self.db_info['host'],
                port=self.db_info['port'],
                user=self.db_info['user'],
                password=self.db_info['password'],
                database=self.db_info['database']
            )
        except pymysql.MySQLError as e:
            print(f"Error connecting to database: {e}")
            self.connection = None  # 确保连接对象为None，如果连接失败

        return self.pool.connection()

    def __enter__(self):
        # 使用上下文管理器时自动打开连接
        self.conn = self.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 使用上下文管理器时自动关闭连接
        self.conn.close()
        self.conn = None


wind = {
        'host': '192.168.7.93',
        'port': 3306,
        'user': 'quantchina',
        'password': 'zMxq7VNYJljTFIQ8',
        'database': 'wind'
    }


if __name__ == "__main__":

    db_connector = DatabaseConnector(wind)
    db_connector.get_connection()

