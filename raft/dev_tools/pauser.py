from raft.dev_tools.pausing_app import TriggerType

class Pauser:
    
    def __init__(self, spec, tcase):
        self.spec = spec
        self.tcase = tcase
        self.am_leader = False
        self.paused = False
        self.broadcasting = False
        self.sent_count = 0
        
    def reset(self):
        self.am_leader = False
        self.paused = False
        self.broadcasting = False
        self.sent_count = 0
        
    async def leader_pause(self, mode, code, message):
        self.am_leader = True
        self.tcase.leader = self.spec
        
        if self.broadcasting or code in ("term_start", "heartbeat"):
            # we are sending one to each follower, whether
            # it is running or not
            limit = len(self.tcase.servers) - 1
        else:
            limit = self.tcase.expected_followers
        self.sent_count += 1
        if self.sent_count < limit:
            self.tcase.logger.debug("not pausing on %s, sent count" \
                                    " %d not yet %d", code,
                                    self.sent_count, limit)
            return True
        self.tcase.logger.info("got sent for %s followers, pausing",
                               limit)
        await self.spec.pbt_server.pause_all(TriggerType.interceptor,
                                             dict(mode=mode,
                                                  code=code))
        return True
    
    async def follower_pause(self, mode, code, message):
        self.am_leader = False
        self.tcase.followers.append(self.spec)
        self.paused = True
        await self.spec.pbt_server.pause_all(TriggerType.interceptor,
                                             dict(mode=mode,
                                                  code=code))
        return True
