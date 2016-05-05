/**
 * @file index service
 * @author xxx
 * @date 2016/04/25
 */
'use strict';

index.service(
    'IndexService',
    [
        '$rootScope', '$http',
        function ($rootScope, $http) {
            this.commonFunc = function () {
                console.log('common func called');
            };
        }
    ]
);
