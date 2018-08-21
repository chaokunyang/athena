# coding=utf-8
"""
netty use big endian default (small end method ends with 'LE'). so python use big endian explicitly
"""
import asyncio
import json
import os
import sys
import traceback

from athena import util


class TaskExecutor:
    op_codes_dict = {
        'HEARTBEAT': b'\x01',
        'TASK_SUBMIT': b'\x02',
        'TASK_SUCCESS': b'\x03',
        'TASK_FAIL': b'\x04',
        'TASK_KILL': b'\x05',
    }

    def __init__(self, task_id, host, port):
        self.logger = util.get_logger(self.__class__.__name__)
        self.task_id = int(task_id)
        self.host = host
        self.port = int(port)
        self.heartbeat_timeout = 3
        self.loop = asyncio.get_event_loop()
        reader, writer = self.loop.run_until_complete(self.connect())
        self.reader = reader
        self.writer = writer
        self.heart_beat_future = None
        self.task_handler_future = None

    def start(self):
        self.logger.info("Start TaskExecutor")
        self.loop.run_until_complete(self.supervisor())
        self.loop.stop()
        self.loop.close()
        self.logger.info("TaskExecutor finished")

    def _stop(self):
        self.heart_beat_future.cancel()
        self.task_handler_future.cancel()
        self.writer.close()

    async def supervisor(self):
        await self.handshake()
        self.heart_beat_future = asyncio.ensure_future(self.heart_beat())
        self.task_handler_future = asyncio.ensure_future(self.task_handler())
        await self.task_handler_future

    async def connect(self):
        reader, writer = await asyncio.open_connection(
            self.host, self.port, loop=self.loop)
        return reader, writer

    async def handshake(self):
        self.logger.info("start task handshake")
        self.writer.write(self.task_id.to_bytes(8, byteorder='big'))
        pid = os.getpid()
        self.writer.write(pid.to_bytes(4, byteorder='big'))
        self.writer.write(b'\x02')  # 对应TaskBackend的PyTaskMessageCodec
        await self.writer.drain()
        self.logger.info("finished task handshake")

    async def task_handler(self):
        while True:
            try:
                op_code = await self.reader.read(1)
                if op_code == self.op_codes_dict['TASK_SUBMIT']:
                    data_length = int.from_bytes(await util.read(self.reader, 4), 'big')  # data length: java int type
                    data = await util.read(self.reader, data_length)
                    task_submit_info = json.loads(data.decode())
                    self.logger.info("Received task submit info %s", task_submit_info)
                    self.loop.run_in_executor(None, self._execute, task_submit_info)
                elif op_code == self.op_codes_dict['TASK_KILL']:
                    self.logger.info("Received kill task command. Exit taskExecutor now")
                    sys.exit(0)
                elif op_code == self.op_codes_dict['HEARTBEAT']:
                    self.logger.debug("HEARTBEAT")
                else:
                    self.logger.error("Error op_codes %s. exit taskExecutor now", op_code)
                    sys.exit(0)
            except asyncio.CancelledError:
                return

    async def heart_beat(self):
        try:
            while True:
                await asyncio.sleep(self.heartbeat_timeout)
                self.logger.info("Send HEARTBEAT message")
                self.writer.write(self.op_codes_dict['HEARTBEAT'])
                try:
                    await self.writer.drain()
                except Exception as e:
                    self.logger.error("heart_beat error: {!r}, close task executor".format(e))
                    sys.exit(0)
        except asyncio.CancelledError:
            return

    def _execute(self, task_submit_info):
        packages = task_submit_info['packages']

        try:
            util.prepare_packages(packages.split(","))
            entry_point = task_submit_info['entry_point']
            task = util.import_code(entry_point)
            self.logger.info("task %s execute started", self.task_id)
            task.execute()
        except Exception:
            self.writer.write(self.op_codes_dict['TASK_FAIL'])
            err_msg = traceback.format_exc()
            self.logger.error("task %s execute error: %s", self.task_id, err_msg)
            # send task failure message and error info
            err_bytes = err_msg.encode()
            self.writer.write(len(err_bytes).to_bytes(4, 'big'))
            self.writer.write(err_bytes)
            # https://stackoverflow.com/questions/39754020/runtime-error-event-loop-is-running
            f = asyncio.run_coroutine_threadsafe(self.writer.drain(), self.loop)
            f.result()
            self.logger.info("Send TaskFail message succeed")
            self._stop()
        else:
            self.logger.info("Task %s execute succeed. ", self.task_id)
            # send task success message
            self.writer.write(self.op_codes_dict['TASK_SUCCESS'])
            f = asyncio.run_coroutine_threadsafe(self.writer.drain(), self.loop)
            f.result()
            self.logger.info("Send TaskSuccess message succeed")
            self._stop()


logger = util.get_logger(os.path.basename(__file__))


def exec_task_action(task_info):
    action = task_info['action']
    task_id = task_info['task_id']
    logger.info("action: %s", action)
    packages = task_info['packages']
    util.prepare_packages(packages.split(","))
    task = util.import_code(task_info['entry_point'])
    if hasattr(task, action):
        logger.info("task %s %s started", task_id, action)
        action_func = getattr(task, action)
        action_func()
        logger.info("task %s %s finished", task_id, action)
    else:
        logger.info("task %s didn't provide %s method, '%s' won't be called", task_id, action, action)
    return task


def main():
    logger.info("args: %s", sys.argv[1:])
    task_info = util.parse_args(sys.argv[1:])
    task_id = task_info['task_id']

    if 'action' not in task_info:
        host, port = task_info['task_manager_host'], task_info['task_manager_port']
        logger.info("task manager: host %s, port: %s", host, port)
        executor = TaskExecutor(task_id, host, port)
        executor.start()
    else:
        exec_task_action(task_info)


if __name__ == '__main__':
    main()
