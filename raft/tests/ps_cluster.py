from __future__ import annotations
import asyncio
import time
import shutil
from pathlib import Path
from multiprocessing import Process
import logging
import dataclasses
from dataclasses import dataclass, field
from typing import Tuple, List, Union
from enum import Enum
import abc

from raft.servers.server import Server
from raft.states.base_state import Substate
from raft.messages.log_pull import LogPullMessage, LogPullResponseMessage
from raft.comms.memory_comms import reset_queues

from raft.tests.log_control import one_proc_log_setup
from raft.tests.pausing_app import PausingBankTellerServer, PausingMonitor
from raft.tests.pausing_app import PausingInterceptor, InterceptorMode
from raft.tests.bt_server import ServerThread

@dataclass
class ServerSpec:
    name: str
    port: int
    addr: Tuple[str, int] = field(repr=False)
    working_dir: Path = field(repr=False)
    run_args: List = field(repr=False)
    pbt_server: PausingBankTellerServer = field(repr=False)
    pbt_server_thread: ServerThread = field(repr=False)
    monitor: PausingMonitor = field(repr=False)
    interceptor: PausingInterceptor = field(repr=False)
    server: ServerThread = field(repr=False, default=None)
    running: bool = field(repr=False, default=False)
    
class PausePoint(str, Enum):
    election_done = "ELECTION_DONE"
    log_pull_straddle = "LOG_PULL_STRADDLE"

class PauseStep(metaclass=abc.ABCMeta):

    def __init__(self, cluster: PausingServerCluster,
                 point: PausePoint):
        self.point = point
        self.cluster = cluster
        self.server_specs = []
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    @abc.abstractmethod
    def configure(self, server_spec: ServerSpec) -> None:
        """ called to setup monitors and interceptors to cause the pause"""
        raise NotImplementedError

    @abc.abstractmethod
    def check_condition(self, server_spec: ServerSpec) -> bool:
        """ called to see if pause has happened """
        raise NotImplementedError
    
class SubstatePauseStep(PauseStep):

    def __init__(self, cluster: PausingServerCluster,
                 point: PausePoint):
        super().__init__(cluster, point)
        self.substates = []
        
    def configure(self, server_spec: ServerSpec, substates):
        self.substates = []
        self.server_specs.append(server_spec)
        method = None
        if hasattr(self, "substate_pause_method"):
            method = self.substate_pause_method
        for substate in substates:
            self.substates.append(substate)
            server_spec.monitor.set_pause_on_substate(substate, method)
    
    def check_condition(self, server_spec: ServerSpec) -> bool:
        return server_spec.pbt_server.paused

    def resume(self, server_spec: ServerSpec):
        for substate in self.substates:
            server_spec.monitor.clear_pause_on_substate(substate)
        if not server_spec.running:
            return
        async def resume():
            await server_spec.pbt_server.resume_all()
        self.loop.run_until_complete(resume())
    
    
class PauseAfterElection(SubstatePauseStep):

    def __init__(self, cluster: PausingServerCluster):
        super().__init__(cluster, PausePoint.election_done)

    def configure(self, server_spec: ServerSpec):
        substates = [Substate.joined, Substate.new_leader,
                    Substate.became_leader]
        super().configure(server_spec, substates)

    async def substate_pause_method(self, monitor, state,
                                    old_substate, new_substate):
        spec = None
        for tspec in self.server_specs:
            if tspec.monitor == monitor:
                spec = tspec
        if spec is None:
            raise Exception("could not locate server spec from monitor")
        async def leader_pause(leader_spec):
            logger = logging.getLogger(__name__)
            logger.info("Waiting in pause for state %s substate %s",
                        state, new_substate)
            while True:
                # wait for the others to be paused
                paused = 0
                expected = 0
                for ospec in self.server_specs:
                    if ospec == spec:
                        continue
                    if ospec.pbt_server.thread.running:
                        expected += 1
                    if ospec.pbt_server.paused:
                        paused += 1
                if paused == expected:
                    break
                await asyncio.sleep(0.001)
            await monitor.substate_pause_method(monitor,
                                                state,
                                                old_substate,
                                                new_substate)
        if new_substate == Substate.became_leader:
            asyncio.create_task(leader_pause(spec))
            return True
        return await monitor.substate_pause_method(monitor, state,
                                                   old_substate, new_substate)
            
            
        
class MessageSplitStep(PauseStep):

    def __init__(self, cluster: PausingServerCluster, point: PausePoint):
        super().__init__(cluster, point)
        self.sender_code = None
        self.receiver_code = None
        
    def configure(self, server_spec: ServerSpec,
                  sender_code, receiver_code):
        self.sender_code = sender_code
        self.receiver_code = receiver_code
        server_spec.interceptor.add_trigger(InterceptorMode.out_after,
                                            sender_code)
        server_spec.interceptor.add_trigger(InterceptorMode.in_before,
                                            receiver_code)
        
    def check_condition(self, server_spec: ServerSpec) -> bool:
        return server_spec.pbt_server.paused

    def resume(self, server_spec: ServerSpec):
        server_spec.interceptor.clear_trigger(InterceptorMode.out_after,
                                              self.sender_code)
        server_spec.interceptor.clear_trigger(InterceptorMode.in_before,
                                              self.receiver_code)
        async def resume():
            await server_spec.pbt_server.resume_all()
        self.loop.run_until_complete(resume())

class LogPullStraddle(MessageSplitStep):

    def __init__(self, cluster: PausingServerCluster):
        super().__init__(cluster, PausePoint.log_pull_straddle)

    def configure(self, server_spec: ServerSpec):
        super().configure(server_spec, LogPullMessage._code,
                        LogPullMessage._code)

        
pause_map = {
    PausePoint.election_done:PauseAfterElection,
    PausePoint.log_pull_straddle:LogPullStraddle,
}
    
class PausingServerCluster:

    def __init__(self, server_count, logging_type=None, base_port=5000):
        self.base_dir = Path("/tmp/raft_tests")
        self.server_count = server_count
        self.logging_type = logging_type
        self.base_port = base_port
        self.server_specs = {}
        self.all_server_addrs = []
        self.pause_stepper = None
        if self.logging_type == "devel_one_proc":
            self.log_config = one_proc_log_setup()
        else:
            logging.getLogger().handlers = []
            self.log_config = None
        self.logger = logging.getLogger(__name__)
        reset_queues()

    def get_servers(self):
        return self.server_specs

    def on_assert_pause_all(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        async def pauser():
            from raft.tests.timer import get_timer_set
            await self.timer_set.pause_all()
            for name, spec in self.server_specs.items():
                spec.pbt_server.comms.pause()
        
    def prepare(self, timeout_basis=1.0):
        if len(self.server_specs) > 0:
            raise Exception("cannot call prepare more than once")
        dir_recs = self.setup_dirs()
        self.all_server_addrs = [ sdef['addr'] for sdef in  dir_recs.values() ]
        for name, dir_rec in dir_recs.items():
            if dir_rec.get("server_thread"):
                raise Exception(f"server {name} server_thread already running")
            others = []
            for addr in self.all_server_addrs:
                if addr[1] != dir_rec['port']:
                    others.append(addr)
            args = [dir_rec['port'], dir_rec['working_dir'],
                    dir_rec['name'], others, self.log_config]
            pbt_server = PausingBankTellerServer(*args)
            smap = pbt_server.state_map
            smap.follower_leaderless_timeout=0.75 * timeout_basis
            smap.candidate_voting_timeout=0.5 * timeout_basis
            smap.leader_heartbeat_timeout=0.5 * timeout_basis
            
            spec = ServerSpec(dir_rec['name'], dir_rec['port'],
                              dir_rec['addr'], dir_rec['working_dir'],
                              run_args=args,
                              pbt_server=pbt_server,
                              pbt_server_thread=pbt_server.thread,
                              monitor=pbt_server.monitor,
                              interceptor=pbt_server.interceptor)
            self.server_specs[spec.name] = spec
        return self.server_specs

    def add_pause_point(self, pause_point: PausePoint,
                        servers: Union[List[str], None] = None,
                        stepper: Union[PauseStep, None] = None):
        if self.pause_stepper:
            raise Exception("must clear pause points before adding one")
        if stepper is None:
            stepper_cls = pause_map.get(pause_point)
            if stepper_cls is None:
                raise Exception(f"No step class for {pause_point}")
            t_stepper = stepper_cls(self)
        else:
            t_stepper = stepper
        if servers is None:
            targets = [name for name in self.server_specs.keys()]
        else:
            targets = servers[::]
        for name in targets:
            spec = self.server_specs[name]
            t_stepper.configure(spec)
        self.pause_stepper = t_stepper

    def wait_for_pause(self, timeout=2, expected_count=None):
        if not self.pause_stepper:
            raise Exception('you must add a pause point before calling')
        start_time = time.time()
        if expected_count is None:
            expected_count = 0
            for name, spec in self.server_specs.items():
                if spec.running:
                    expected_count += 1
        paused_count = 0
        while time.time() - start_time < timeout:
            paused_count = 0
            for name, spec in self.server_specs.items():
                if not spec.running:
                    continue
                if self.pause_stepper.check_condition(spec):
                  paused_count += 1  
            if paused_count == expected_count:
                break
            time.sleep(0.001)
        self.logger.debug("waited %f of %f, expected %d, got %d",
                          time.time()-start_time, timeout,
                          expected_count, paused_count)
        return paused_count == expected_count

    def resume_and_add_pause_point(self, pause_point: PausePoint,
                                   servers: Union[List[str], None] = None,
                                   stepper: Union[PauseStep, None] = None):
        if not self.pause_stepper:
            raise Exception('you must add a pause point before calling')
        orig = self.pause_stepper
        self.pause_stepper = None
        self.add_pause_point(pause_point, servers, stepper)
        for name, spec in self.server_specs.items():
            orig.resume(spec)
        
    def resume_from_pause(self):
        if not self.pause_stepper:
            raise Exception('you must add a pause point before calling')
        for name, spec in self.server_specs.items():
            self.pause_stepper.resume(spec)
        self.pause_stepper = None
            
    def start_all_servers(self):
        for name in self.server_specs.keys():
            self.start_one_server(name)

    def start_one_server(self, name):
        spec = self.server_specs[name]
        server_thread = spec.pbt_server.start_thread()
        spec.pbt_server.configure()
        spec.pbt_server.start()
        spec.running = True

    def setup_dirs(self):
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir)
        self.base_dir.mkdir()
        result = {}
        for i in range(self.server_count):
            name = f"server_{i}"
            wdir = Path(self.base_dir, name)
            if wdir.exists():
                shutil.rmtree(wdir)
            wdir.mkdir()
            port = self.base_port + i
            addr = ('localhost', port)
            result[name] = dict(name=name, working_dir=wdir,
                                port=port, addr=addr)
        return result

    def get_server_by_addr(self, addr):
        for name, spec in self.server_specs.items():
            if spec.addr == addr:
                return spec
        return None

    def stop_server(self, name):
        spec = self.server_specs[name]
        if spec.pbt_server_thread:
            spec.pbt_server_thread.stop()
            spec.pbt_server_thread = None
        spec.pbt_server.stop()            
        spec.running = False

    def stop_all_servers(self):
        for name in self.server_specs.keys():
            self.stop_server(name)

