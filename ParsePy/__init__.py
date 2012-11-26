#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import base64
import requests
import sys 

try:
    import simplejson as json
except ImportError:
    import json

import datetime
from collections import defaultdict

API_ROOT = 'https://api.parse.com/1'

#Should we move these to a config file? 
APPLICATION_ID = ''
REST_API_KEY = ''

class ParseBinaryDataWrapper(str):
    pass


class ParseBase(object):
    def __init__(self):
        self.headers = {
            'Content-Type': 'application/json',
            'X-Parse-Application-Id': APPLICATION_ID,
            'X-Parse-REST-API-Key': REST_API_KEY
            }
        self.USER_LOGGED_IN = False

    def _executeCall(self, uri, http_verb, data=None, api_type=None):
        if api_type: 
            url = '/'.join([API_ROOT, api_type])
        else:
            url = API_ROOT
        url = '/'.join([url, uri.strip('/')])

        if http_verb is 'POST':
            response = requests.post(url, data=data, headers=self.headers)
        elif http_verb is 'GET':
            response = requests.get(url, params=data, headers=self.headers)
        elif http_verb is 'PUT':
            response = requests.put(url, data=data, headers=self.headers)
        elif http_verb is 'DELETE':
            response = requests.delete(url, headers=self.headers)

        #response_dict = json.loads(response.read())
        response_dict = json.loads(response.text)

        if 'error' in response_dict: 
            print ('>> Parse API returned error: "{}" '
                'on {} request to url "{}"').format(response_dict['error'], http_verb, url)
            if data: 
                print 'Attached data: ', data
        
        return response_dict

    def _ISO8601ToDatetime(self, date_string):
        # TODO: verify correct handling of timezone
        date_string = date_string[:-1] + 'UTC'
        date = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%Z")
        return date

    def _login(self, username, password):
        '''
        Login Functionality implemented at ParseBase Level to allow (for 
        example) queries to be contructed as a user. See Parse 
        documentation: https://www.parse.com/docs/rest#users-login
        '''

        login_params = {}
        login_params['username'] = username
        login_params['password'] = password
        
        uri = '/login?'
        try:
            response_dict = self._executeCall(uri, 'GET', login_params)
        except: 
            print 'Login Failed: ', sys.exc_info()[0]
            raise
        
        self.USER_LOGGED_IN = True

        self.user = {}
        self.user['username'] = response_dict['username']
        self.user['user_object_id'] = response_dict['objectId']
        
        self.headers['X-Parse-Session-Token'] = response_dict['sessionToken']
        
        print '>> User {} logged in.'.format(self.user['username'])
        return self

class ParseObject(ParseBase):
    def __init__(self, class_name, attrs_dict=None):
        super(ParseObject, self).__init__()
        self._class_name = class_name
        self._object_id = None
        self._updated_at = None
        self._created_at = None

        if attrs_dict:
            self._populateFromDict(attrs_dict)

    def objectId(self):
        return self._object_id

    def updatedAt(self):
        return self._updated_at and self._ISO8601ToDatetime(self._updated_at) or None

    def createdAt(self):
        return self._created_at and self._ISO8601ToDatetime(self._created_at) or None

    def save(self):
        if self._object_id:
            self._update()
        else:
            self._create()

    def delete(self):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: DELETE

        uri = '%s/%s' % (self._class_name, self._object_id)

        self._executeCall(uri, 'DELETE')

        self = self.__init__(None)

    def _populateFromDict(self, attrs_dict):
        try:
            self._object_id = attrs_dict['objectId']
            self._created_at = attrs_dict['createdAt']
            self._updated_at = attrs_dict['updatedAt']

            del attrs_dict['objectId']
            del attrs_dict['createdAt']
            del attrs_dict['updatedAt']
        except KeyError:
            pass

        attrs_dict = dict(map(self._convertFromParseType, attrs_dict.items()))

        self.__dict__.update(attrs_dict)

    def _convertToParseType(self, prop):
        key, value = prop

        if type(value) == ParseObject:
            value = {'__type': 'Pointer',
                     'className': value._class_name,
                     'objectId': value._object_id}
        elif type(value) == datetime.datetime:
            value = {'__type': 'Date',
                    #'iso': value.isoformat()[:-3] + 'Z'} # take off the last 3 digits and add a Z
                    'iso': value.strftime("%Y-%m-%dT%H:%M:%S.%f%Z")}
        elif type(value) == ParseBinaryDataWrapper:
            value = {'__type': 'Bytes',
                     'base64': base64.b64encode(value)}

        return (key, value)

    def _convertFromParseType(self, prop):
        key, value = prop

        if type(value) == dict and '__type' in value:
            if value['__type'] == 'Pointer':
                value = ParseQuery(value['className']).get(value['objectId'])
            elif value['__type'] == 'Date':
                value = self._ISO8601ToDatetime(value['iso'])
            elif value['__type'] == 'Bytes':
                value = ParseBinaryDataWrapper(base64.b64decode(value['base64']))
            else:
                raise Exception('Invalid __type.')

        return (key, value)

    def _getJSONProperties(self):
        properties_list = self.__dict__.items()

        # filter properties that start with an underscore
        properties_list = filter(lambda prop: prop[0][0] != '_', properties_list)

        #properties_list = [(key, value) for key, value in self.__dict__.items() if key[0] != '_']

        properties_list = map(self._convertToParseType, properties_list)

        properties_dict = dict(properties_list)
        json_properties = json.dumps(properties_dict)

        return json_properties

    def _create(self):
        # URL: /1/classes/<className>
        # HTTP Verb: POST

        uri = '%s' % self._class_name

        data = self._getJSONProperties()

        response_dict = self._executeCall(uri, 'POST', data)

        self._created_at = self._updated_at = response_dict['createdAt']
        self._object_id = response_dict['objectId']

    def _update(self):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: PUT

        uri = '%s/%s' % (self._class_name, self._object_id)

        data = self._getJSONProperties()

        response_dict = self._executeCall(uri, 'PUT', data)

        self._updated_at = response_dict['updatedAt']


class ParseQuery(ParseBase):
    def __init__(self, class_name):
        super(ParseQuery, self).__init__()
        self._class_name = class_name
        self._where = defaultdict(dict)
        self._options = {}
        self._object_id = ''

    def eq(self, name, value):
        self._where[name] = value
        return self

    # It's tempting to generate the comparison functions programatically,
    # but probably not worth the decrease in readability of the code.
    def lt(self, name, value):
        self._where[name]['$lt'] = value
        return self

    def lte(self, name, value):
        self._where[name]['$lte'] = value
        return self

    def gt(self, name, value):
        self._where[name]['$gt'] = value
        return self

    def gte(self, name, value):
        self._where[name]['$gte'] = value
        return self

    def ne(self, name, value):
        self._where[name]['$ne'] = value
        return self

    def order(self, order, decending=False):
        # add a minus sign before the order value if decending == True
        self._options['order'] = decending and ('-' + order) or order
        return self

    def limit(self, limit):
        self._options['limit'] = limit
        return self

    def skip(self, skip):
        self._options['skip'] = skip
        return self

    def get(self, object_id):
        self._object_id = object_id
        return self._fetch(single_result=True)

#    def login(self, *args): 
#        return self.

    def fetch(self):
        # hide the single_result param of the _fetch method from the library user
        # since it's only useful internally
        return self._fetch()

    def _fetch(self, single_result=False):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: GET

        if self._object_id:
            uri = '/%s/%s' % (self._class_name, self._object_id)
            options = None
        else:
            options = dict(self._options)  # make a local copy
            if self._where:
                for key, value in self._where.iteritems():
                    if isinstance(value, ParseObject):
                        self._where[key] = {'__type': 'Pointer',
                                            'className': value._class_name,
                                            'objectId': value._object_id}
                # JSON encode WHERE values
                where = json.dumps(self._where)
                options.update({'where': where})

            uri = '/%s' % (self._class_name)

        #For Debugging (temp)
        print 'Executing call'
        print (uri, 'GET', options, 'classes')
        response_dict = self._executeCall(uri, 'GET', options, api_type='classes')
        
        try: 
          if single_result:
                return ParseObject(self._class_name, response_dict)
          else:
              return [ParseObject(self._class_name, result) for result in response_dict['results']]
        except: 
          print "Unable to return result, ResponseDict: ", response_dict

class ParseNotification(ParseBase):
    def push(self, channel='', type='ios', data=None):
        # '' for broadcast
        # type for device type(ios or android)
        # data for all alert and self-defined info
        if not isinstance(data, dict):
            return
        post_data = {'channel': channel, 'type': type, 'data': data}
        result = self._executeCall('', 'POST', type='push', data=json.dumps(post_data))
        return result


