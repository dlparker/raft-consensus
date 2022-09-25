import logging
from logging.config import dictConfig
from pathlib import Path

from log_server import LogSocketServer

have_logging_server = False

def config_server_logging(filepath):
    lfstring = '%(process)s %(asctime)s [%(levelname)s] %(name)s: %(message)s'
    log_formaters = dict(standard=dict(format=lfstring))
    file_handler = dict(level="DEBUG",
                        formatter="standard",
                        encoding='utf-8',
                        filename=str(filepath))
    file_handler['class'] = "logging.FileHandler"
    log_handlers = dict(file=file_handler)
    the_log = dict(handlers=['file'], level="DEBUG", propagate=True)
    log_loggers = dict()
    log_loggers[''] = the_log
    log_config = dict(version=1, disable_existing_loggers = True,
                      formatters=log_formaters,
                      handlers=log_handlers,
                      loggers=log_loggers)
    return log_config
    
def config_logging(logfile_path, use_server=False, server_filepath=None):
    lfstring = '%(process)s %(asctime)s [%(levelname)s] %(name)s: %(message)s'
    log_formaters = dict(standard=dict(format=lfstring))
    file_handler = dict(level="DEBUG",
                        formatter="standard",
                        encoding='utf-8',
                        mode='w',
                        filename=str(logfile_path))
    file_handler['class'] = "logging.FileHandler"
    stdout_handler =  dict(level="DEBUG",
                           formatter="standard",
                           stream="ext://sys.stdout")
    # can't us "class" in above form
    stdout_handler['class'] = "logging.StreamHandler"
    #    socket_handler = logging.handlers.SocketHandler('localhost', 9999)
    log_handlers = dict(file=file_handler, stdout=stdout_handler)
    handler_names = ['file', 'stdout']
    server_config = None
    if use_server:
        socket_handler  = dict(level="DEBUG",
                               host="localhost",
                               port=9999)
        socket_handler['class'] = "logging.handlers.SocketHandler"
        log_handlers['sock'] = socket_handler
        handler_names.append("sock")
        server_config = config_server_logging(server_filepath)
    root_log = dict(handlers=handler_names, level="INFO", propagate=True)
    log_loggers = dict()
    log_loggers[''] = root_log
    raft_log = dict(handlers=handler_names, level="INFO", propagate=False)
    log_loggers['raft'] = raft_log
    follower_log = dict(handlers=handler_names, level="DEBUG", propagate=False)
    log_loggers['raft.states.follower'] = follower_log
    log_loggers['raft.states.leader'] = follower_log
    log_config = dict(version=1, disable_existing_loggers = True,
                      formatters=log_formaters,
                      handlers=log_handlers,
                      loggers=log_loggers)
    return log_config, server_config

def setup_logging_for_test(name, file_path="/tmp/raft_tests/test.log",
                           use_server=True, server_filepath="/tmp/combined_raft.log"):
    config,server_config = config_logging(file_path,  use_server=use_server,
                                         server_filepath=server_filepath)

    # this is the place to modify the logging config if needed for debugging test failure
    dictConfig(config)
    global have_logging_server
    if use_server and not have_logging_server:
        LogSocketServer.start(port=9999, configDict=server_config)
        print("started logging server")
        have_logging_server = True
    logging.getLogger("test_control").info("starting test %s", name)
    return config
    
def stop_logging_server():
    global have_logging_server
    if have_logging_server:
        LogSocketServer.stop()
        have_logging_server = False
    
    
if __name__=="__main__":
    config,server_config = setup_logging("/tmp/first.log", True, "/tmp/server.log")
    dictConfig(config)
    logger = logging.getLogger()
    for i in range(10):
        logger.debug("log rec %d", i)
    import time
    time.sleep(0.5)
    LogSocketServer.stop()
    from pprint import pprint
    pprint(config)
    pprint(server_config)
