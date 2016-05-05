# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render
from django.http import JsonResponse
from django.shortcuts import render_to_response
import MonitorRpc

def home(request):
    dic = {'YL':'hello'}
    return JsonResponse(dic)

def monitor(request):
    monitor = MonitorRpc.MonitorImpl()
    flag = monitor.AddMonitorRpc(request)
    dic = {}
    dic['status'] = 'ok' 
    return JsonResponse(dic);

