__author__ = 'hzyuxin'

import yaml
import os
import sh
import logging
import pymongo
from collections import defaultdict
logging.basicConfig(
    format='%(asctime)-15s%(levelname)s:%(message)s', level=logging.INFO)


class MongoSeverManager(object):

    def __init__(self, fp):
        with open(fp) as f:
            self.yaml = yaml.load(f)
            self.cmd = []
            self.mongods = []
            self.parse_config()
            self.repl_sets = defaultdict(list)

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

    def run(self):
        for c in self.cmd:
            c.prepare()
            c.cmd()
            c.wait()
        logging.info('boot all mongods finished')

        self.init_repl_sets()
        self.sharding()
        logging.info('all sharding done')
        self.loop()

    def loop(self):
        while True:
            sh.sleep(10)

    def init_repl_sets(self):
        for repl_set_name, repl_set in self.repl_sets.items():

            # 1.construct init command
            command = {}
            command["_id"] = repl_set_name
            command['members'] = []
            for i, mongod in enumerate(repl_set):
                member = {}
                member['_id'] = i
                member['host'] = "%s:%d" % (mongod.hostname, mongod.port)
                command['members'].append(member)
            logging.info("init set command \n %s", repr(command))

            # 2.connect to one mongod for set init
            one_mongod = repl_set[0]
            one_mongod_name = "%s:%d" % (one_mongod.hostname, one_mongod.port)
            conn = pymongo.Connection(one_mongod_name)
            db = conn.admin
            logging.info('init repl set %s', one_mongod_name)

            # 3.init set
            db.command('replSetInitiate', command)

    def sharding(self):
        conn = pymongo.Connection('localhost', self.mongos.port)
        db = conn.admin
        # add no repl set mongod
        for mongod in self.mongods:
            if not mongod.is_set:
                shard_address = ':'.join(['localhost', str(mongod.port)])
                logging.info('add sharding address %s', shard_address)
                try:
                    db.command('addshard', shard_address)
                except pymongo.errors.OperationFailure:
                    logging.warn('%s already sharded', shard_address)
            else:
                pass

        # add repl set mongod
        sh.sleep(60)
        for mongod_set in self.repl_sets.values():
            try:
                shard_address = mongod_set[1].replset_name
                db.command('addshard', shard_address)
            except pymongo.errors.OperationFailure:
                logging.warn('%s already sharded', shard_address)


class MongoCmd(object):

    def __init__(self, d, mgr):
        self.data = d
        self.mgr = mgr
        self.bind()
        self.ok = False
        self.logf = open(self.logname, 'wb+')

    def cmd(self):
        return 'TODO'

    def prepare(self):
        if hasattr(self, 'path') and not os.path.isdir(self.path):
            logging.info('mkdir ' + self.path)
            sh.mkdir('-p', self.path)

    def bind(self):
        for k, v in self.data.items():
            if v:
                setattr(self, k, v)

    def wait(self):
        while not self.ok:
            sh.sleep(self.interval)

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
        ip = sh.hostname('-i').strip()
        configdb_path = ':'.join([ip, str(self.mgr.config.port)])

        sh.mongos(
            port=self.port, configdb=configdb_path, _bg=True, _out=self.log_redirect)


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
        self.priority = 4
        self.hostname = sh.cat('/etc/hostname').strip()

    def cmd(self):
        logging.info('start one mongod')
        if self.is_set:
            self.replset_name = "%s/%s:%d" % (self.setname,
                                              self.hostname, self.replset)
            sh.mongod(
                dbpath=self.path, port=self.port, smallfiles=True, _bg=True, _out=self.log_redirect, replSet=self.replset_name)
            self.mgr.repl_sets[self.setname].append(self)
        else:
            sh.mongod(
                dbpath=self.path, port=self.port, smallfiles=True, _bg=True, _out=self.log_redirect)


if __name__ == '__main__':
    mongo_mgr = MongoSeverManager('config.yaml')
    mongo_mgr.run()
