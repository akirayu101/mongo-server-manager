__author__ = 'hzyuxin'

import yaml
import os
import sh


class MongoSeverManager(object):

    def __init__(self, fp):
        with open(fp) as f:
            self.yaml = yaml.load(f)
            self.cmd = []
            self.mongods = []
            self.parse_config()

    def parse_config(self):
        for cmd_dict in self.yaml:
            if 'mongod' in cmd_dict:
                mongod = MongoMongod(cmd_dict, self)
                self.cmd.append(mongod)
                self.mongods.append(mongod)
            elif 'config' in cmd_dict:
                self.config = MongoConfig(cmd_dict, self)
                self.cmd.append(self.config)
            elif 'mongos' in cmd_dict:
                self.mongos = MongoMongos(cmd_dict, self)
                self.cmd.append(self.mongos)
            else:
                Exception('unsupported cmd type')
        self.cmd = sorted(self.cmd, key=lambda cmd: cmd.priority, reverse=True)


class MongoCmd(object):

    def __init__(self, d, mgr):
        self.data = d
        self.mgr = mgr
        self.bind()

    def cmd(self):
        return 'TODO'

    def precmd(self):
        if hasattr(self, 'path') and not os.path.isdir(self.path):
            sh.mkdir('-p', self.path)

    def bind(self):
        for k, v in self.data.items():
            if v:
                setattr(self, k, v)

    def postcmd(self):
        sh.sleep(5)


class MongoMongos(MongoCmd):

    def __init__(self, d, mgr):
        super(MongoMongos, self).__init__(d, mgr)
        self.priority = 2

    def cmd(self):
        sh.nohup(
            'mongos',
            port=self.port, configdb='localhost:' + str(self.mgr.config.port), _bg=True)


class MongoConfig(MongoCmd):

    def __init__(self, d, mgr):
        super(MongoConfig, self).__init__(d, mgr)
        self.priority = 3

    def cmd(self):
        sh.nohup(
            'mongod',
            dbpath=self.path, port=self.port, smallfiles=True, _bg=True)


class MongoMongod(MongoCmd):

    def __init__(self, d, mgr):
        super(MongoMongod, self).__init__(d, mgr)
        self.priority = 1

    def cmd(self):
        sh.nohup(
            'mongod',
            dbpath=self.path, port=self.port, smallfiles=True, _bg=True)


if __name__ == '__main__':
    mongo_mgr = MongoSeverManager('config.yaml')
    for c in mongo_mgr.cmd:
        c.precmd()
        c.cmd()
        c.postcmd()
    
    while True:
        pass
