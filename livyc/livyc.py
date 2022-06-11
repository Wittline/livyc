from livyc.LivyC.pysparksession import PySparkSession
from livyc.LivyC.sparksessionerror import SparkSessionError

class LivyC:

    def __init__(self, *args):

        self.args_dict = {
            'livy_server_url':None, 
            'port':None,
            'jars':''
            }

        try:
            self.args = dict(args)
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
            session = PySparkSession(livy_server_url=self.args['livy_server_url'] + ":" + self.args['port'])
            session.jars = ','.join(self.args['jars'])
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