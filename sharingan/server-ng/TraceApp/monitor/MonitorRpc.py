from django.http import HttpResponse
from datetime import datetime
import time
import json

from TraceAPI import scheduler_pb2
from sofa.pbrpc import client

class MonitorImpl(object):
    def __init__(self):
        self.name = 'monitor'
        self.service_addr = 'nj03-ps-global-build168.nj03.baidu.com:11111'
    
    def ConvertRule(self, rule, pb_rule):
        expression = rule.get('expression')
        pb_rule.expr.type = expression.get('type') 
        pb_rule.expr.expr = expression.get('expr') 
        pb_rule.expr.column_delim = expression.get('col_delim') 
        pb_rule.expr.column_idx = int(expression.get('col_idx'))
        
        record_list = rule.get('record')
        for record in record_list:
            pb_record = pb_rule.record_vec.add()
            pb_record.op = record.get('op')
            pb_record.type = record.get('type')
            pb_record.key = record.get('key')
            pb_record.key_name = record.get('key_name')

    def AddMonitorRpc(self, request):
        req = json.loads(request.GET.get('json_param'))    
        
        pb_request = scheduler_pb2.RpcMonitorRequest()
        pb_request.db_name = req['db_name'] 
        pb_request.table_name = req['table_name'] 
        pb_request.moduler_owner.extend(req['mail_list'])
        rule_set = req['rule_set']
        
        result = rule_set['result']
        self.ConvertRule(result, pb_request.rule_set.result) 
        
        rule_list = rule_set['rule_list']
        for rule in rule_list:
            pb_rule = pb_request.rule_set.rule_list.add()
            self.ConvertRule(rule, pb_rule) 
        
        rpc_channel = client.Channel(self.service_addr)
        sdk = scheduler_pb2.LogSchedulerService_Stub(rpc_channel)
        controller = client.Controller()   
        controller.SetTimeout(5)
        pb_response = sdk.RpcMonitor(controller, pb_request)

        return True


