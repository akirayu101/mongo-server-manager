__author__ = 'hzyuxin'

import yaml
import os
import sh
import logging
logging.basicConfig(
    format='%(asctime)-15s%(levelname)s:%(message)s', level=logging.INFO)


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
        self.ok = False
        self.logf = open(self.logname, 'wb+')

    def cmd(self):
        return 'TODO'

    def precmd(self):
        if hasattr(self, 'path') and not os.path.isdir(self.path):
            logging.info('mkdir ' + self.path)
            sh.mkdir('-p', self.path)

    def bind(self):
        for k, v in self.data.items():
            if v:
                setattr(self, k, v)

    def postcmd(self):
        while not self.ok:
            sh.sleep(self.time)

    @property
    def success_msg(self):
        return 'waiting for connections'

    def log_redirect(self, msg):
        if self.success_msg in msg and not self.ok:
            self.ok = True
            logging.info(str(self.port) + ' listen success')
        self.logf.write(msg)
        self.logf.flush()


class MongoMongos(MongoCmd):

    def __init__(self, d, mgr):
        super(MongoMongos, self).__init__(d, mgr)
        self.priority = 2

    def cmd(self):
        logging.info('start main mongos')
        sh.mongos(
            port=self.port, configdb='localhost:' + str(self.mgr.config.port), _bg=True, _out=self.log_redirect)


class MongoConfig(MongoCmd):

    def __init__(self, d, mgr):
        super(MongoConfig, self).__init__(d, mgr)
        self.priority = 3

    def cmd(self):
        logging.info('start config mongod')
        sh.mongod(
            dbpath=self.path, port=self.port, smallfiles=True, _bg=True, _out=self.log_redirect)


class MongoMongod(MongoCmd):

    def __init__(self, d, mgr):
        super(MongoMongod, self).__init__(d, mgr)
        self.priority = 1

    def cmd(self):
        logging.info('start one mongod')
        sh.mongod(
            dbpath=self.path, port=self.port, smallfiles=True, _bg=True, _out=self.log_redirect)


if __name__ == '__main__':
    mongo_mgr = MongoSeverManager('config.yaml')
    for c in mongo_mgr.cmd:
        c.precmd()
        c.cmd()
        c.postcmd()

    for c in mongo_mgr.cmd:
        print c.ok
