"""
  At one point this was identical to the bank_app.py file in the bank_teller
  demonstration program, because it was copied here from there. Expected to 
  diverge over time.
"""
from typing import Union
import logging
from raftframe.app_api.app import AppAPI, CommandResult
from raftframe.log.log_api import RecordCode

class BankingApp(AppAPI):

    def __init__(self, server=None):
        self.server = server
        self.logger = logging.getLogger(__name__)

    def set_server(self, server):
        self.server = server
        
    async def execute_command(self, command) -> CommandResult:
        parsed = command.split()
        log_response = True
        extra_dict = None
        log = self.server.get_log()
        last_rec = log.read()
        while last_rec and last_rec.code != RecordCode.client:
            if last_rec.index == 1:
                last_rec = None
                break
            last_rec = log.read(last_rec.index - 1)
        if last_rec:
            balance = last_rec.user_data['balance'] or 0
        else:
            balance = 0
        if len(parsed) == 0:
            response = "Invalid parsed"
            log_response = False
        elif len(parsed) == 1 and parsed[0] == 'log_stats':
            response = "log_stats result"
            log_response = False
            extra_dict = dict(commit_index=log.get_commit_index(),
                              term=log.get_term())
            if last_rec:
                extra_dict['last_index'] = last_rec.index
            else:
                extra_dict['last_index'] = None
        elif len(parsed) == 1 and parsed[0] == 'query':
            response = "Your current account balance is: " + str(balance)
        elif len(parsed) == 2 and parsed[0] == 'credit':
            if int(parsed[1]) <= 0:
                response = "Credit amount must be positive"
            else:
                response = "Successfully credited " + parsed[1] + " to your account"
                balance += int(parsed[1])
        elif len(parsed) == 2 and parsed[0] == 'debit':
            if balance >= int(parsed[1]):
                if int(parsed[1]) <= 0:
                    response = "Debit amount must be positive"
                else:
                    response = "Successfully debited " + parsed[1] + " from your account"
                    balance -= int(parsed[1])
            else:
                response = "Insufficient account balance"
        else:
            response = "Invalid command"
            log_response = False
        if response == "Invalid command":
            self.logger.debug("invalid client command %s", command)
            data = {
                "command": command,
                "balance": None,
                "response": response,
            }
        else:
            self.logger.debug("completed client command %s", command)
            data = {
                "command": command,
                "balance": balance,
                "response": response,
            }
        if extra_dict is not None:
            data.update(extra_dict)
        result = CommandResult(command=command,
                               response=data,
                               log_response=log_response)
        return result
        



        
    
