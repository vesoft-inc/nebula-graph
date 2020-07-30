# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import random
import subprocess
import time
import shutil
import socket
import glob
import signal
from contextlib import closing

NEBULA_START_COMMAND_FORMAT = "bin/nebula-{} --flagfile conf/nebula-{}.conf {}"


class NebulaManager(object):
    def __init__(self, build_dir, src_dir):
        self.build_dir = build_dir
        self.src_dir = src_dir
        self.work_dir = "/tmp/nebula-" + str(
            random.randrange(1000000, 100000000))
        self.pids = {}

    def set_test_dir(self, work_dir):
        self.work_dir = work_dir

    def _copy_nebula_conf(self):
        graph_path = self.build_dir + '/bin'
        graph_conf_path = self.src_dir + '/conf'
        storage_path = self.src_dir + '/modules/storage/src/daemons/_build'
        storage_conf_path = self.src_dir + '/modules/storage/conf'

        # graph
        shutil.copy(graph_path + '/nebula-graphd', self.work_dir + '/bin/')
        shutil.copy(graph_conf_path + '/nebula-graphd.conf.default',
                    self.work_dir + '/conf/nebula-graphd.conf')
        # storage
        shutil.copy(storage_path + '/nebula-storaged', self.work_dir + '/bin/')
        shutil.copy(storage_conf_path + '/nebula-storaged.conf.default',
                    self.work_dir + '/conf/nebula-storaged.conf')
        # meta
        shutil.copy(storage_path + '/nebula-metad', self.work_dir + '/bin/')
        shutil.copy(storage_conf_path + '/nebula-metad.conf.default',
                    self.work_dir + '/conf/nebula-metad.conf')

    def _format_nebula_command(self, name, meta_port, ports):
        param_format = "--meta_server_addrs={} --port={} --ws_http_port={} --ws_h2_port={} -v=4"
        param = param_format.format("127.0.0.1:" + str(meta_port), ports[0],
                                    ports[1], ports[2])
        command = NEBULA_START_COMMAND_FORMAT.format(name, name, param)
        return command

    def _find_free_port(self):
        ports = []
        for i in range(0, 3):
            with closing(socket.socket(socket.AF_INET,
                                       socket.SOCK_STREAM)) as s:
                s.bind(('', 0))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                ports.append(s.getsockname()[1])
        return ports

    def install(self):
        os.mkdir(self.work_dir)
        print("created work directory:" + self.work_dir)
        os.chdir(self.work_dir)
        installed_files = ['logs', 'bin', 'conf', 'data', 'pids', 'scripts']
        for f in installed_files:
            os.mkdir(self.work_dir + '/' + f)
        self._copy_nebula_conf()

    def start(self):
        os.chdir(self.work_dir)

        metad_ports = self._find_free_port()
        command = ''
        graph_port = 0
        for server_name in ['metad', 'storaged', 'graphd']:
            ports = []
            if server_name != 'metad':
                ports = self._find_free_port()
            else:
                ports = metad_ports
            command = self._format_nebula_command(server_name, metad_ports[0],
                                                  ports)
            print("exec: " + command)
            p = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE)
            p.wait()
            if p.returncode != 0:
                print("error: " + p.communicate()[0])
            else:
                graph_port = ports[0]

        # wait nebula start
        time.sleep(8)
        for pf in glob.glob(self.work_dir + '/pids/*.pid'):
            with open(pf) as f:
                pid = int(f.readline())
                self.pids[f.name] = pid

        return graph_port

    def stop(self, cleanup):
        print("try to stop nebula services...")
        for p in self.pids:
            try:
                os.kill(self.pids[p], signal.SIGTERM)
            except OSError as err:
                print("nebula stop " + p + " failed: " + str(err))
        time.sleep(3)
        if cleanup:
            shutil.rmtree(self.work_dir)
