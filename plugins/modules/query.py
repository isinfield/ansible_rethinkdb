#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020, Kenneth Sinfield
#
# Author: Ken Sinfield <isinfield on github>
#
# GNU General Public License v2.0 (see COPYING or https://www.gnu.org/licenses/gpl-2.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

ANSIBLE_METADATA = {
    'metadata_version': '1.0',
    'status': ['beta'],
    'supported_by': 'Ken Sinfield'
}

DOCUMENTATION = '''
---
module: query
short_description: Query a RethinkDB database
description:
    - Run a REQL query against a RethinkDB database
version_added: "2.10"
author:
    - Ken Sinfield (@isinfield)
options:
    host:
        description:
            - The hostname or IP address (v4 or v6) of the RethinkDB host
        required: True
        type: str
    port:
        description:
            - The TCP port to connect to on the RedthinkDB host
        required: False
        type: int
        default: 28015
    user:
        description:
            - The RethinkDB user to connect with
        required: True
        type: str
    password:
        description:
            - The RethinkDB user's password
        required: True
        type: str
    query:
        description:
            - The REQL query statement
            - Requires the db is specified. See examples below
        required: True
        type: str
notes:
    - https://rethinkdb.com/docs/guide/python/
    - All queries must start with db
    - Do no include .run() at the end
requirements:
    - rethinkdb
'''

EXAMPLES = '''
- hosts: localhost
  connection: local
  gather_facts: no
  tasks:
  - name: Select all RethinkDB users
    kensinfield.rethinkdb.query:
      host: "{{host}}"
      user: "{{user}}"
      password: "{{password}}"
      port: "{{port}}"
      query: "db('rethinkdb').table('users')"

  - name: Create a table in the DB named test
    kensinfield.rethinkdb.query:
      host: "{{host}}"
      user: "{{user}}"
      password: "{{password}}"
      port: "{{port}}"
      query: "db('test').table_create('authors')"
'''

RETURN = '''
data:
    description: A list of returned documents
    type: list
    returned: success
msg:
    description: A useful message
    type: str
    returned: fail
'''

from ansible.module_utils.basic import AnsibleModule
from rethinkdb import RethinkDB
import re
# Python3 workaround for unicode function
try:
    unicode('')
except NameError:
    unicode = str


def check_args(module):
    regexPattern1 = r"^db\(.*?\).*"
    result = re.search(regexPattern1, module.params.get('query'), re.MULTILINE)
    if result is None:
        module.fail_json("Invalid query format. The query must by valid REQL starting with db(table).")


def execute_query(module):
    results = list()

    try:
        r = RethinkDB()
        r.connect(host=module.params.get('host'),
                  port=module.params.get('port'),
                  user=module.params.get('user'),
                  password=module.params.get('password')).repl()
        query_result = eval("r.{0}.run()".format(module.params.get('query')))
        if type(query_result) is dict:
            results.append(query_result)
        else:
            for document in query_result:
                results.append(document)
        return results
    except Exception as e:
        module.fail_json(msg="Error: {0}".format(e))


def main():
    """
    Main function
    :returns: RethinkDB Query Results
    """
    module = AnsibleModule(
        argument_spec=dict(
            host=dict(required=True, type='str'),
            port=dict(default=28015, type='int'),
            user=dict(required=True, type='str'),
            password=dict(required=True, type='str'),
            query=dict(required=True, type='str')
        ),
        supports_check_mode=True
    )
    check_args(module)
    return_value = execute_query(module)
    module.exit_json(data=return_value)


if __name__ == '__main__':
    main()
