/**
 * @file index router
 * @author xxx
 * @date 2016/04/25
 */
'use strict';

index.config(
    [
        '$routeProvider',
        '$locationProvider',
        function ($routeProvider, $locationProvider) {
            //$locationProvider.html5Mode(true);
            // url router
            $routeProvider.when(
                '/test1', {
                    templateUrl: '/static/index/tpls/test1.tpl.html',
                    controller: 'IndexController'
                }
            ).when(
                '/test2', {
                    templateUrl: '/static/index/tpls/test2.tpl.html',
                    controller: 'IndexController'
                }
            );
        }
    ]
);
