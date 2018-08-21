# coding=utf-8
import asyncio
import json
import os
import sys
import traceback
import pickle

from athena import util
from athena.task_executor import exec_task_action


class TaskSupportServer:
    def __init__(self, host, port):
        self.logger = util.get_logger(self.__class__.__name__)
        self.host = host
        self.port = int(port)
        self.loop = asyncio.get_event_loop()
        self.server = None

    def start(self):
        self.server = self.loop.run_until_complete(
            asyncio.start_server(self.handle_task_action, self.host, self.port, loop=self.loop))
        print('Serving on {}'.format(self.server.sockets[0].getsockname()))
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()

    async def handle_task_action(self, reader, writer):
        task_info_length = int.from_bytes(await util.read(reader, 4), 'big')
        task_info_data = await util.read(reader, task_info_length)
        task_info = json.loads(task_info_data.decode())
        self.logger.info("Received task info %s", task_info)
        action = task_info['action']
        if action == 'init':
            task = exec_task_action(task_info)
        else:
            task_pickle_length = int.from_bytes(await util.read(reader, 4), 'big')
            task_pickle_data = await util.read(reader, task_pickle_length)
            task = pickle.loads(task_pickle_data)

        action_func = getattr(task, action)

        def func():
            action_func()
            task_pickle_data = pickle.dumps(task)
            writer.write(len(task_pickle_data).to_bytes(8, byteorder='big'))
            writer.write(task_pickle_data)
            f = asyncio.run_coroutine_threadsafe(writer.drain(), self.loop)
            f.result()
            print("Close the client socket")
            writer.close()

        self.loop.run_in_executor(None, func, task_info)


def main():
    args = util.parse_args(sys.argv[1:])
    server = TaskSupportServer(args['host'], args['port'])
    server.start()


if __name__ == '__main__':
    main()
