'use strict';

monitor.controller(
    'MonitorController',
    [
        '$scope',
        '$http',
        function ($scope, $http) {
            $scope.names = []; 
            $scope.conf = {}; 
            $scope.conf.db_name = ''; 
            $scope.conf.table_name = ''; 
            $scope.conf.mail_list = [];

            $scope.conf.rule_set = {};
            $scope.conf.rule_set.rule_list = [];
            
            $scope.conf.rule_set.result = {};
            $scope.conf.rule_set.result.expression = {};
            $scope.conf.rule_set.result.record = [];
            
            $scope.AddOwner = function () {
                $scope.conf.mail_list.push($scope.mailer) 
            }
            
            $scope.AddResultRecord = function (index) {
                var record = {key_name: "", key: "", type: "", op: ""};
                $scope.conf.rule_set.result.record.push(record);
            }
            
            $scope.AddRule = function() {
                var rule = {expression: {}, record:[]};
                $scope.conf.rule_set.rule_list.push(rule);
            }

            $scope.AddRecord = function (index) {
                var record = {key_name: "", key: "", type: "", op: ""};
                $scope.conf.rule_set.rule_list[index].record.push(record);
            }

            $scope.SubmitConf = function() {
                var jdata = angular.toJson($scope.conf)
                $http.get(
                    '/monitor/',
                    {params: {json_param: jdata} 
                    }).success(function(data) {
                        $scope.submit_res = data;
                        }
                    );
            }
            
        }
    ]
);
