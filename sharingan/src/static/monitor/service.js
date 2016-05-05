'use strict';

monitor.service(
    'MonitorService',
    [
        '$rootScope', '$http',
        function ($rootScope, $http) {
            this.commonFunc = function () {
                console.log('common func called');
            };
        }
    ]
);
