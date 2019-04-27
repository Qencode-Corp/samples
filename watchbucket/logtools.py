import logging
import os

LOG_FORMAT = '[%(asctime)s] %(levelname)s | (PID: {0}  %(filename)s:%(lineno)s )| %(message)s'.format(os.getpid())


class Logger(object):
  FORMATTER = logging.Formatter(LOG_FORMAT)
  logging.root.setLevel(logging.NOTSET)
  ABS_PATH = os.path.dirname(os.path.abspath(__file__))

  def __init__(self, file_name, path=None, log_dir=None):
    self.path = path
    self.log_dir = log_dir
    self.log_file = self._prepare_path(file_name)
    self.hdlr = logging.FileHandler(self.log_file, 'a')
    self.hdlr.setFormatter(self.FORMATTER)
    self.logger = logging.getLogger(self.log_file)
    self.logger.addHandler(self.hdlr)

  def _prepare_path(self, file_name):
    if self.path:
      path = '{0}/{1}/{2}.log'.format(self.ABS_PATH, self.path, file_name)
    elif self.log_dir:
      path = '{0}/{1}.log'.format(self.log_dir, file_name)
    else:
      path = '{0}/{1}.log'.format(self.ABS_PATH, file_name)
    return path

  def log(self):
    return self.logger

def logger(*args, **kwargs):
  log = Logger(*args, **kwargs)
  return log.log()

