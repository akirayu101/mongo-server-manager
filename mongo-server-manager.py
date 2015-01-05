__author__ = 'hzyuxin'

import yaml
import os

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
        self.cmd = sorted(self.cmd, key = lambda cmd:cmd.priority, reverse=True)


class MongoCmd(object):
    def __init__(self, d, mgr):
        self.data = d
        self.mgr = mgr
        self.bind()

    def cmd(self):
        return 'TODO'

    def bind(self):
        for k, v in self.data.items():
            if v:
                setattr(self, k, v)

class MongoMongos(MongoCmd):
    def __init__(self, d, mgr):
        super(MongoMongos, self).__init__(d, mgr)
        self.priority = 2

    def cmd(self):
        pass

class MongoConfig(MongoCmd):
    def __init__(self, d, mgr):
        super(MongoConfig, self).__init__(d, mgr)
        self.priority = 3

    def cmd(self):
        pass

class MongoMongod(MongoCmd):
    def __init__(self, d, mgr):
        super(MongoMongod, self).__init__(d, mgr)
        self.priority = 1

    def cmd(self):
        pass



if __name__ == '__main__':
    mongo_mgr = MongoSeverManager('config.yaml')
    pass

