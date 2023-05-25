"""
@author Team 63, Melbourne, 2023

Hanying Li (1181148) Haichi Long (1079593) Ji Feng (1412053)
Jiayao Lu (1079059) Xinlin Li (1068093)
"""

class _const:
    class ConstError(PermissionError):
        pass

    class ConstCaseError(ConstError):
        pass

    # rewrite __setattr__() 
    def __setattr__(self, name, value):
        if name in self.__dict__:  # can not reassign
            raise self.ConstError("Can't change const {0}".format(name))
        if not name.isupper():  # need upper
            raise self.ConstCaseError("const name {0} is not all uppercase".format(name))
        self.__dict__[name] = value

const = _const()

const.USERNAME = 'admin'
const.PASSWORD = 'password'
const.IP = '172.26.135.30'
const.PORT = '5984'
const.URL = f'http://{const.USERNAME}:{const.PASSWORD}@{const.IP}:{const.PORT}/'


const.COFFEE_KEYWORD = ['coffee', 'cappuccino', 'espresso', 'french press',
                'barista', 'americano', 'nespresso vertuo', 'macchiato',
                'flat white', 'cold brew', 'nespresso pods', 'starbucks drinks',
                'moka', 'nescafe', 'latte', 'arabica', 'cafe au lait',
                'frappuccino', 'decaf', 'sanka', 'batch brew', 
                'long black', 'mocha', 'cafe' ]

const.WORK_KEYWORD = ['work', 'job', 'employment', 'career',
                'workplace', 'task', 'duty', 'workload',
                'deadline', 'client', 'project', 'office']

const.CLIMATE_KEYWORD = ["climate", "ClimateCrisis", "ClimateCriminals",
                    "ClimateActionNow", "ClimateApe", "climatechange",
                    "ClimateStrike", "Climate", "ClimateEmergency", "ClimateChange"]

const.SYD_TIME = {'NSW': 'Australia/Sydney',
            'VIC': 'Australia/Sydney',
            'QLD': 'Australia/Sydney',
            'ACT': 'Australia/Sydney',
            'TAS': 'Australia/Sydney'}

const.ADE_TIME  ={'SA': 'Australia/Adelaide',
            'NT': 'Australia/Adelaide'}

const.PER_TIME = {'WA': 'Australia/Perth'}