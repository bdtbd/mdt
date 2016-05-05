/**
 * @file index controller
 * @author xxx
 * @date 2016/04/25
 */
'use strict';

index.controller(
    'IndexController',
    [
        '$scope',
        '$http',
        function ($scope, $http) {
            $scope.value = '123456';
        }
    ]
);
