(function () {
    'use strict';

    angular
        .module("itsyouonline.registration")
        .service("registrationService", ['$http', RegistrationService]);

    function RegistrationService($http) {
        return {
            validateUsername: validateUsername,
            requestValidation: requestValidation,
            register: register,
            getLogo: getLogo,
            getDescription: getDescription,
            resendValidation: resendValidation
        };

        function validateUsername(username) {
            var options = {
                params: {
                    username: username
                }
            };
            return $http.get('/validateusername', options);
        }

        function requestValidation(firstname, lastname, email, phone, password) {
            var url = '/register/validation';
            var data = {
                firstname: firstname,
                lastname: lastname,
                email: email,
                phone: phone,
                password: password,
                langkey: localStorage.getItem('langKey')
            };
            return $http.post(url, data);
        }

        function register(firstname, lastname, email, emailcode, sms, phonenumbercode, password, redirectparams) {
            var url = '/register?' + redirectparams;
            var data = {
                firstname: firstname,
                lastname: lastname,
                email: email.trim(),
                emailcode: emailcode,
                phonenumber: sms,
                phonenumbercode: phonenumbercode,
                password: password,
                redirectparams: redirectparams,
                langkey: localStorage.getItem('langKey')
            };
            return $http.post(url, data);
        }

        function getLogo(globalid) {
            var url = '/api/organizations/' + encodeURIComponent(globalid) + '/logo';
            return $http.get(url).then(
                function (response) {
                    return response.data;
                },
                function (reason) {
                    return $q.reject(reason);
                }
            );
        }


        function getDescription(globalId, langKey) {
            var url = '/api/organizations/' + encodeURIComponent(globalId) + '/description/' + encodeURIComponent(langKey) + '/withfallback';
            return $http.get(url).then(
                function (response) {
                    return response.data;
                },
                function (reason) {
                    return $q.reject(reason);
                }
            );
        }

        function resendValidation(email, phone) {
          var url = '/register/resendvalidation';
          var data = {
              email: email,
              phone: phone,
              langkey: localStorage.getItem('langKey')
          };
          return $http.post(url, data);
        }
    }
})();
