# A class to hold User States and their links to neuroarch queries

import time

# memory management
# 1) have a maximum number of states per list
# 2) clear a State that have not been accessed in x hours
# 3)

class State(object):
    def __init__(self,name,limit=10):
        """ start with an empty state list """
        self.state = []
        self.messages = []
        self.last_accessed = 0
        self.accessed()
        self.garbage = []
        self.limit = limit
        self.clean_up_time = 60 * 60 # One hour

    def memory_management(self):
        # Delete everything for a user, if they have not accessed it in a long amount of time.
        if int(time.time()) - self.last_accessed > self.clean_up_time: # One minute
            self.clear()
            self.force_empty_garbage()
            print("memory managed")
            return True
        else:
            return False

    def append(self,reference):
        self.accessed()
        self.state.append(reference)
        # Enforce list size
        if len(self.state) > self.limit:
            self.state = self.state[-self.limit:]

    def accessed(self):
        self.last_accessed = int(time.time())

    def clear(self):
        self.accessed()
        self.garbage.extend(self.state)
        self.state = []

    def revert(self, index):
        """ revert to a previous state, requires that the index is at least one less than the length of the list """
        assert type(index) is int
        assert index >= 0
        assert index < len(self.state)-1
        self.garbage.extend(self.state[index+1:])
        self.state = self.state[:index+1]
        return self.state[-1]

    def retrieve(self,index=0):
        """ treating the list as filo, 0 returns the end of the list """
        self.accessed()
        if self.state == []:
            return None
        else:
            assert len(self.state) > index
            return self.state[-1-index]

    def force_empty_garbage(self):
        """ remove all garbage list """
        del self.garbage
        self.garbage = []

    def process_command(self,action):
        """ process a command from the nlp server """
        """ expect format : {'undo':{states:1}}"
            viable commands:
            restart
            undo """
        assert len(action.keys()) == 1
        command = list(action.keys())[0]
        if command == 'restart':
            self.clear()
        elif command == 'undo':
            to_state = len(self.state) - int(action[command]['states']) -1
            self.revert(to_state)
        elif command == 'retrieve':
            return self.retrieve(int(action[command]['state']))
        elif command == 'swap':
            assert(len(action[command]['states'])==2)
            i, j = action[command]['states']
            self.state[-1-i], self.state[-1-j] = self.state[-1-j],self.state[-1-i]
            return self.retrieve(j)
        else:
            self.messages.append("action failed : " + str(action))
        return self.retrieve()
