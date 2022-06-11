import pkgutil
from livyc.LivyC.sparksession import SparkSession
import json
import pandas as pd
import random
import inspect
from pathlib import PurePath

class PySparkSession(SparkSession):
    def __init__(self, livy_server_url=None):
        super().__init__(livy_server_url, kind='pyspark')
        self._tmp_var_name = '_tmp_' + str(id(self)) + '_' + str(random.randint(100, 100000))
        self.run('import pyspark; import json')

    def read(self, code: str):        

        code = '{} = {}; type({}).__name__'.format(self._tmp_var_name, code,
                                                   self._tmp_var_name)
        z_type = self._unquote(self.run(code))

        if z_type in ('int', 'float', 'bool'):
            z = self.run(self._tmp_var_name)
            return eval(z_type)(z)

        if z_type == 'str':
            z = self.run(self._tmp_var_name)
            return self._unquote(z)

        if z_type == 'DataFrame':
            code = """\
                for _livy_client_serialised_row in {}.toJSON().collect():
                    print(_livy_client_serialised_row)""".format(
                self._tmp_var_name)
            output = self.run(code)

            rows = []
            for line in output.split('\n'):
                if line:
                    rows.append(json.loads(line))
            z = pd.DataFrame.from_records(rows)
            return z

        try:
            z = self.run('json.dumps({})'.format(self._tmp_var_name))
            zz = json.loads(self._unquote(z))
            return eval(z_type)(zz)
        except Exception as e:
            raise Exception(
                'failed to fetch an "{}" object: {};  additional error info: {}'.
                format(z_type, z, e))

    def _unquote(self, s: str) -> str:
        return s[1:][:-1]

    def _relative_path(self, path: str) -> str:

        if path.startswith('/'):
            return path

        caller = inspect.getframeinfo(inspect.stack()[1][0]).filename
        assert caller.endswith('.py')
        p = PurePath(caller).parent
        while True:
            if path.startswith('./'):
                path = path[2:]
            elif path.startswith('../'):
                path = path[3:]
                p = p.parent
            else:
                break
        return str(p.joinpath(path))                      
				
    def run_module(self, module_name: str) -> None:
        pack = pkgutil.get_loader(module_name)
        self.run_file(pack.path)
		

    def run_file(self, file_path: str) -> str:        
        assert file_path.startswith('/')
        code = open(file_path, 'r').read()
        return self.run(code)


    def run_function(self, fname: str, *args, **kwargs):        
        code = f'{fname}(*{args}, **{kwargs})'
        return self.read(code)