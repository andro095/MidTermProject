from hdfs import InsecureClient


class HDFSWriter(InsecureClient):
    def __init__(self, host: str, port: int = 9870, user: str = None, root: str = None, **kwargs):
        super().__init__(f'http://{host}:{port}', user=user, root=root, **kwargs)

    def save(self, path: str, data: str, append: bool = False, overwrite: bool = False, encoding: str = 'utf-8'):
        with self.write(path, encoding=encoding, append=append, overwrite=overwrite) as writer:
            writer.write(data)