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