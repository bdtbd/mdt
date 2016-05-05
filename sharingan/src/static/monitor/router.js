
'use strict';

monitor.config(
    [
        '$routeProvider',
        '$locationProvider',
        function ($routeProvider, $locationProvider) {
            //$locationProvider.html5Mode(true);
            // url router
            $routeProvider.when(
                '/monitor', {
                    templateUrl: '/static/monitor/tpls/monitor.tpl.html',
                    controller: 'MonitorController'
                }
            );
        }
    ]
);

