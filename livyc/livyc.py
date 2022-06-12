import requests
import pkgutil
import json
import pandas as pd
import random
import inspect
from pathlib import PurePath
import textwrap
import time
from typing import Optional

class JsonClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({'X-Requested-By': 'ambari'})

    def close(self) -> None:
        self.session.close()

    def get(self, endpoint: str = '') -> dict:
        return self._request('GET', endpoint)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request('POST', endpoint, data)

    def delete(self, endpoint: str = '') -> dict:
        return self._request('DELETE', endpoint)

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        url = self.url.rstrip('/') + endpoint
        response = self.session.request(method, url, json=data)        
        response.raise_for_status()
        return response.json()

class SparkSessionError(Exception):
    def __init__(self, name, value, traceback, kind) -> None:
        self._name = name
        self._value = value
        self._tb = traceback
        self._kind_script = kind
        super().__init__(name, value, traceback)

    def __str__(self) -> str:
        kind = 'Spark' if self._kind_script == 'spark' else 'PySpark'
        return '{}: {} error while processing submitted code.\nname: {}\nvalue: {}\ntraceback:\n{}'.format(
            self.__module__ + '.' + self.__class__.__name__, kind, self._name,
            self._value, ''.join(self._tb))        

class SparkSession:
    def __init__(self,
                 livy_server_url=None,
                 jars: str = '',
                 _kind_script: str = 'pyspark'
                 ):

        assert _kind_script in ('pyspark')
        

        self._host = self._get_livy_host(livy_server_url)
        self._client = JsonClient(self._host)
        self._kind_script = _kind_script
        self.jars = jars
        self._start()        

    def _wait(self, endpoint: str, wait_for_state: str):        
        intervals = self._polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)
        while True:
            rr = self._client.get(endpoint)
            if rr['state'] == wait_for_state:
                return rr
            time.sleep(next(intervals))

    def _start(self):   
        r = self._client.post('/sessions', data={'kind': self._kind_script, 'conf': {"spark.jars.ivy":"/opt/bitnami/spark/ivy/", "spark.jars.packages": self.jars} })
        session_id = r['id']
        self._session_id = session_id
        self._wait('/sessions/{}'.format(session_id), 'idle')
 

    def __del__(self):
        if hasattr(self, '_session_id'):
            try:
                self._client.delete('/sessions/{}'.format(self._session_id))
            except Exception as ex:
                raise ex
            finally:
                pass

    def _get_livy_host(self, livy_server_url: Optional[str] = None):        
        
        idx = livy_server_url.find('://')
        if idx >= 0:
            livy_server_url = livy_server_url[(idx + 3):]

        return 'http://' + livy_server_url


    def _polling_intervals(self, start, rest, max_duration = None):
        def _intervals():
            yield from start
            while True:
                yield rest

        cumulative = 0.0
        for interval in _intervals():
            cumulative += interval
            if max_duration is not None and cumulative > max_duration:
                break
            yield interval            				


    def run(self, code: str) -> str:        
        data = {'code': textwrap.dedent(code)}
        r = self._client.post(
            '/sessions/{}/statements'.format(self._session_id), data=data)
        statement_id = r['id']
        z = self._wait(
            '/sessions/{}/statements/{}'.format(self._session_id,
                                                statement_id), 'available')
        output = z['output']
        if output['status'] == 'error':
            raise SparkSessionError(output['ename'], output['evalue'],
                                    output['traceback'], self._kind_script)
        assert output['status'] == 'ok'
        return output['data']['text/plain']


class PySparkSession(SparkSession):
    def __init__(self, livy_server_url=None, jars = ''):
        super().__init__(livy_server_url, jars, _kind_script='pyspark')
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

class LivyC:

    def __init__(self, *args):

        self.args_dict = {
            "livy_server_url":None, 
            "port":None,
            "jars":''
            }

        try:
            self.args = dict(args[0])
            if len(self.args.keys()) == len(self.args_dict.keys()):
                for k in self.args_dict.keys():
                    self.args_dict[k] = self.args[k]
            else:
                print("There are keys missing")                  
        except TypeError:
            raise            
    
    
    def create_session(self):
        if self.args['livy_server_url'] is not None \
            and self.args['port'] is not None:
            session = PySparkSession(livy_server_url=self.args['livy_server_url'] + ":" + self.args['port'], jars= ','.join(self.args['jars']))
            return session
        else:
            print("URL or PORT INCORRECT")
            raise        
    

    def run_script(self,  session, script):
        try:
            if isinstance(session, PySparkSession):
                return session.run(script)
            else:
                print("PySparkSession is needed")
                raise
        except SparkSessionError as e:
            raise

    def run_function(self, session, fname, *args, **kwargs):
        try:
            if isinstance(session, PySparkSession):
                return session.run_function(fname, *args, **kwargs)
            else:
                print("PySparkSession is needed")
                raise                            
        except SparkSessionError as e:
            raise

    def run_file(self, session, file_path):
        try:
            if isinstance(session, PySparkSession):
                return session.run_file(file_path)
            else:
                print("PySparkSession is needed")
                raise
        except SparkSessionError as e:
            raise         

    def read_variable(self, session, variable):
        try:
            if isinstance(session, PySparkSession):
                return session.read(variable)   
            else:
                print("PySparkSession is needed")
                raise
        except SparkSessionError as e:
            raise