from hdfs import InsecureClient


class HDFSWriter:
    def __init__(self, host: str, port: int = 9870, user: str = None):
        self.client = InsecureClient(f'http://{host}:{port}', user=user)

    def save(self, path: str, data: str, append: bool = False, overwrite: bool = False, encoding: str = 'utf-8'):
        with self.client.write(path, encoding=encoding, append=append, overwrite=overwrite) as writer:
            writer.write(data)