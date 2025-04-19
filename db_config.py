class DBConfig:
    def __init__(self, format: str, jdbc_url: str, table_name: str, user: str, password: str, driver: str):
        self.format = format
        self.jdbc_url = jdbc_url
        self.table_name = table_name
        self.user = user
        self.password = password,
        self.driver = driver