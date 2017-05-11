import configparser
import os


class Config(object):
    def __init__(self):
        self.conf = 'default.conf'
        if not os.path.exists(self.conf):
            config = configparser.ConfigParser()
            config['database'] = {'host': 'localhost',
                                  'user': 'root',
                                  'passwd': '369212',
                                  'name': 'path_restore'}
            config['analizeTime'] = {
                'homepath': '/home/elvis/map/analize/analizeTime/countXEntTime/'}
            with open(self.conf, 'w') as configfile:
                config.write(configfile)

    def getConf(self, section):
        config = configparser.ConfigParser()
        config.read('default.conf')
        if section in config.sections():
            return config[section]
        else:
            raise NameError('not have the section: [{}]'.format(section))
