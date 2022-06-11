import textwrap
import time
import os
from typing import Optional
from livyc.LivyC.jsonclient import JsonClient
from livyc.LivyC.sparksessionerror import SparkSessionError


class SparkSession:
    def __init__(self,
                 livy_server_url=None,
                 _kind_script: str = 'pyspark'):

        assert _kind_script in ('pyspark')
        

        self._host = self._get_livy_host(livy_server_url)
        self._client = JsonClient(self._host)
        self._kind_script = _kind_script
        self.jars = None
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