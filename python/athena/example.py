# example tasks. task must provide execute method
def init():
    print("init")


def execute():
    print("execute")


def on_success():
    print("on_success")


def on_error():
    print("on_error")


def on_lost():
    print("on_lost")


def on_killed():
    print("on_killed")


class ExampleTask:
    def init(self):
        print("init ", self.__class__)

    def execute(self):
        print("execute ", self.__class__)

    def on_success(self):
        print("on_success ", self.__class__)


class FailTask:
    def init(self):
        print("init ", self.__class__)

    def execute(self):
        print("execute ", self.__class__)
        raise RuntimeError("execute failure")

    def on_error(self):
        print("on_error ", self.__class__)


class TimeoutTask:
    def init(self):
        print("init ", self.__class__)

    def execute(self):
        print("execute ", self.__class__)
        import time
        time.sleep(60)


class PySparkTask:
    def init(self):
        print("init ", self.__class__)

    def execute(self):
        print("execute ", self.__class__)
        from pyspark.context import SparkContext
        from pyspark.sql import SparkSession
        sc = SparkContext(appName='test PySparkTask')
        b = sc.broadcast([1, 2, 3, 4, 5])
        sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
        spark = SparkSession.builder \
            .master("local") \
            .appName("Word Count") \
            .getOrCreate()
        data = [('Alice', 1), ('Monica', 2)]
        spark.createDataFrame(data).collect()
        spark.createDataFrame(data, ['name', 'age']).collect()

    def on_success(self):
        print("on_success ", self.__class__)

    def on_error(self):
        print("on_error ", self.__class__)

    def on_lost(self):
        print("on_lost ", self.__class__)

    def on_killed(self):
        print("on_killed ", self.__class__)


if __name__ == '__main__':
    task = TimeoutTask()
    task.execute()
