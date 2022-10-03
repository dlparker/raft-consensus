from typing import Union
import logging
from .app import App

class BankingApp(App):

    def __init__(self, server=None):
        self.server = server
        self.logger = logging.getLogger(__name__)

    def set_server(self, server):
        self.server = server
        
    def execute_command(self, command) -> Union[dict, None]:
        parsed = command.split()
        log = self.server.get_log()
        last_rec = log.read()
        if last_rec:
            balance = last_rec.user_data['balance'] or 0
        else:
            balance = 0
        if len(parsed) == 0:
            response = "Invalid parsed"
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
        if response == "Invalid command":
            self.logger.debug("invalid client command %s", command)
            return None
        else:
            self.logger.debug("completed client command %s", command)
        data = {
            "command": command,
            "balance": balance,
            "response": response,
        }
        return data
        



        
    
