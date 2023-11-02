import pymysql


# 链接数据库
def connect_to_database(db_info):
    """
    连接到数据库
    :param db_info: 数据库连接信息
    :return: 数据库连接对象
    """
    host = db_info['host']
    port = db_info['port']
    username = db_info['username']
    password = db_info['password']
    database = db_info['database']

    try:
        # 连接到MySQL数据库
        conn = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")