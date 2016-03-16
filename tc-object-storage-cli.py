#!/usr/bin/env python

import os
import json
import requests
import argparse
import ConfigParser

config_filename = "../setup.ini"

class SwiftClient:
    def __init__(self):
        self._read_args()
        self._read_conf()

        self.project_id = self.args.project_id
        if self.project_id is None:
            self.project_id = self.conf.get('default', 'project_id') 

        self.keystone_endpoint = self.conf.get('object_storage', 'keystone_endpoint')
        self.swift_endpoint = self.conf.get('object_storage', 'object_storage_endpoint')    

        self.username = self.conf.get('default', 'username')
        self.password = self.conf.get('object_storage', 'password')
        
        self.headers = dict()   
        self.headers['Content-Type'] = 'application/json'
        
        token, self.tenant_id = self._get_token()
        print token
        self.headers['X-Auth-Token'] = token

    def _read_args(self):
        self.parser = argparse.ArgumentParser()

        self.parser.add_argument('-p', '--project-id', dest='project_id', 
            help="TOAST Cloud Project ID")

        self.args = self.parser.parse_args()

    def _read_conf(self):
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(config_filename)

    def _get_token(self):
        request = dict()
        auth = dict()
        passwordCredentials = dict()
        passwordCredentials['username'] = self.username
        passwordCredentials['password'] = self.password
        auth['passwordCredentials'] = passwordCredentials
        auth['tenantName'] = self.project_id
        request['auth'] = auth

        url = self.keystone_endpoint + '/identity/v2.0/tokens'
        response = requests.post(url, data=json.dumps(request), headers=self.headers)

        response_body = response.json()
        token = response_body['access']['token']['id']
        tenant_id = response_body['access']['token']['tenant']['id']

        return token, tenant_id

    def get_object_metadata(self, container, object_name):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        url = url + '/' + object_name
        print url

        response = requests.head(url, headers=self.headers) 
        response.raise_for_status()

        print response.status_code
        print response.headers

    def get_objects(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container

        #params = dict()
        #params['format'] = 'json'

        response = requests.get(url, headers=self.headers)  
        response.raise_for_status()

        print response.status_code
        print response.text
        #print response.json()
        #object_list = response.text.split('\n')
        #for each_object in object_list:
        #   print "object: " + each_object

    def upload_object(self, path, filename):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + path + filename

        fp = open(filename, 'r')
        headers = self.headers
        headers['Content-Type'] = 'multipart/formed-data'

        response = requests.put(url, headers=headers, files={'file':fp})
        print response.status_code
        print response.headers

    def upload_object_deprecated(self, path, filename):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + path + filename

        print url

        fp = open(filename, 'r')
        headers = self.headers
        headers['Content-Type'] = 'text/plain'
        print headers

        size = os.path.getsize(filename)
        if size < 1024:
            buf = fp.read(1024)

            response = requests.put(url, headers=headers, data=buf)
            response.raise_for_status()

            print response.status_code
        else:
            """ Upload segments """
            segment_number = 0
            digit = len(str(int(round(size / 1024) + 1)))
            
            while True:
                buf = fp.read(1024)
                if buf:
                    segment_url = url + '/' + str(segment_number).zfill(digit)
                    print segment_url
                    response = requests.put(segment_url, headers=headers, data=buf)
                    response.raise_for_status()
                    segment_number = segment_number + 1
                else:
                    break

            """ Create a manifest file """
            manifest = path + filename
            manifest = manifest.split('/', 1)[1]
            headers['X-Object-Manifest'] = manifest
            print headers['X-Object-Manifest']
            print url
            response = requests.put(url, headers=headers, data='')
            response.raise_for_status()
    
    def download_object(self, path):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + path

        print url

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        print response.text

    def delete_object(self, path):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + path

        print url

        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()

        print response.status_code

    def get_container_metadata(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        print url
        response = requests.head(url, headers=self.headers)

        print response.status_code
        print response.text
        print response.headers

        #print response.headers['x-account-object-count']

    def create_container(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        response = requests.put(url, headers=self.headers)

        print response.status_code

    def delete_container(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        response = requests.delete(url, headers=self.headers)

        print response.status_code

    def run(self):
        return None 

def main():
    swiftclient = SwiftClient()
    swiftclient.get_objects('/')
    #swiftclient.get_container_metadata('/backup')
    #swiftclient.create_container('/joonghyunlee')
    swiftclient.get_objects('/backup_segments')
    swiftclient.get_object_metadata('/backup_segments', 'joonghyunlee/')
    swiftclient.get_container_metadata('/backup_segments/joonghyunlee')
    #swiftclient.upload_object('/backup_segments/joonghyunlee/', 'test.file')
    #swiftclient.upload_object('/backup_segments/joonghyunlee/', 'err.out')
    #swiftclient.delete_object('/backup_segments/joonghyunlee//test.file')
    swiftclient.download_object('/backup_segments/joonghyunlee/err.out')
    #swiftclient.delete_container('/joonghyunlee')
    #swiftclient.get_objects('/')
    #swiftclient.create_container('TT/Fuck')

if __name__ == '__main__':
    main()
