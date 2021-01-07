
import sys, traceback
import re
import pickle
import os
import argparse
from collections import Counter
import time
import txaio
from math import isnan
from configparser import ConfigParser
import uuid
from itertools import islice
import numbers

import six
import numpy as np
import simplejson as json
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.logger import Logger

import autobahn
from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from twisted.internet import reactor, threads
from autobahn.wamp.exception import ApplicationError
from autobahn.wamp.types import RegisterOptions
from autobahn.wamp import auth
from autobahn.websocket.protocol import WebSocketClientFactory
from operator import itemgetter

from pyorient.exceptions import PyOrientConnectionException
from pyorient.ogm import Graph, Config
import pyorient.ogm.graph
from pyorient.serializations import OrientSerialization

# from neuroarch.models import *
import neuroarch.models as models
from neuroarch.query import QueryWrapper, QueryString, _list_repr
import neuroarch.na as na

# User access
import state

# Grab configuration from file
root = os.path.expanduser("/")
home = os.path.expanduser("~")
filepath = os.path.dirname(os.path.abspath(__file__))
config_files = []
config_files.append(os.path.join(home, ".ffbo/config", "ffbo.neuroarch_component.ini"))
config_files.append(os.path.join(root, ".ffbo/config", "ffbo.neuroarch_component.ini"))
config_files.append(os.path.join(home, ".ffbo/config", "config.ini"))
config_files.append(os.path.join(root, ".ffbo/config", "config.ini"))
config_files.append(os.path.join(filepath, "..", "config.ini"))
config = ConfigParser()
configured = False
file_type = 0
for config_file in config_files:
    if os.path.exists(config_file):
        config.read(config_file)
        configured = True
        break
    file_type += 1
if not configured:
    raise Exception("No config file exists for this component")

user = config["USER"]["user"]
secret = config["USER"]["secret"]
ssl = eval(config["AUTH"]["ssl"])
websockets = "wss" if ssl else "ws"
if "ip" in config["SERVER"]:
    ip = config["SERVER"]["ip"]
else:
    ip = "localhost"
if ip in ["localhost", "127.0.0.1"]:
    port = config["NLP"]["port"]
else:
    port = config["NLP"]["expose-port"]
url =  "{}://{}:{}/ws".format(websockets, ip, port)
realm = config["SERVER"]["realm"]
authentication = eval(config["AUTH"]["authentication"])
debug = eval(config["DEBUG"]["debug"])
ca_cert_file = config["AUTH"]["ca_cert_file"]
intermediate_cert_file = config["AUTH"]["intermediate_cert_file"]

# Required to handle dill's inability to serialize namedtuple class generator:
setattr(pyorient.ogm.graph, 'orientdb_version',
        pyorient.ogm.graph.ServerVersion)


def byteify_py2(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                                for key, value in input.items()}
    elif isinstance(input, list):
                return [byteify(element) for element in input]
    elif isinstance(input, unicode):
                return input.encode('utf-8')
    else:
        return input

def byteify_py3(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                                for key, value in input.items()}
    elif isinstance(input, list):
                return [byteify(element) for element in input]
    else:
        return input

def byteify(input):
    if six.PY2:
        return byteify_py2(input)
    else:
        return byteify_py3(input)

def chunks(data, SIZE=1000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

# class graph_connection(object):
#     def __init__(self, database='na_server', username='root', password='root'):
#         try:
#             self.graph = Graph(Config('localhost', 2424,
#                                       username, password, database, 'plocal',
#                                       initial_drop=False,
#                                       serialization_type=OrientSerialization.Binary))
#
#         except:
#             #print "WARNING: Serialisation flag ignored"
#             self.graph = Graph(Config('localhost', 2424,
#                                       username, password, database, 'plocal',
#                                       initial_drop=False))
#         self.graph.include(models.Node.registry)
#         self.graph.include(models.Relationship.registry)

class neuroarch_server(object):
    """ Methods to process neuroarch json tasks """

    def __init__(self, db, user = None, debug = False):
        self.graph = db.graph
        self.user = user
        self.debug = debug
        print('DEBUG:', debug)
        self.query_processor = query_processor(self.graph, debug = debug)
        self._busy = False

    def retrieve_neuron(self,nid):
        # WIP: Currently retrieves all information for the get_as method, this will be refined when we know what data we want to store and pull out here
        try:
            n = self.graph.get_element(nid)
            if n == None:
                return {}
            else:
                output = QueryWrapper.from_objs(self.graph,[n], debug = self.debug)
                return output.get_as(edges = False)[0].to_json()
        except Exception as e:
            raise e



    def process_query(self,task):
        """ configure a task processing, and format the results as desired """
        # WIP: Expand type of information that can be retrieved
        assert 'query' in task
        try:
            self.query_processor.process(task['query'],self.user)
            return True
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            print("An error occured during 'process_query':\n" + tb)
            return False

    @staticmethod
    def process_verb(output, user, verb):
        if verb == 'add':
            # assert(len(user.state)>=2)
            if len(user.state) >= 2:
                user.state[-1] = output+user.state[-2]
            else:
                pass
        elif verb == 'keep':
            assert(len(user.state)>=2)
            user.state[-1] = output & user.state[-2]
            output = user.state[-2] - user.state[-1]
        elif verb == 'remove':
            assert(len(user.state)>=2)
            user.state[-1] = user.state[-2] - output
        elif verb == 'color':
            if isinstance(output, list) and len(output) == 0:
                user.state.pop(-1)
                if len(user.state) > 1:
                    output = user.state[-1] - user.state[-2]
                else:
                    output = user.state[-1]
            else:
                user.state.pop(-1)
        else:
            assert(len(user.state)>=2)
            cmd = {'undo':{'states':1}}
            output = output & user.state[-2]
            user.process_command(cmd)
        return output

    def receive_task(self,task, threshold=None, query_results=True):
        """ process a task of form
            {'query':...} or {'command': ...}
            update the user states, and query neuroarch
            This is the default access route
        """
        while(self._busy):
            time.sleep(1)
        try:
            self._busy = True
            if not type(task) == dict:
                task = json.loads(task)
            task = byteify(task)

            if 'format' not in task:
                task['format'] = 'morphology'

            assert 'query' in task or 'command' in task

            user = self.user
            if 'command' in task:
                output = user.process_command(task['command'])
                if 'verb' in task and not task['verb'] == 'show':
                    try:
                        output = self.process_verb(output, user, task['verb'])
                    except Exception as e:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                        print("An error occured during 'process_verb':\n" + tb)
                    if not task['verb'] == 'add':
                        if task['format'] == 'morphology':
                            output = output.get_data_rids(cls='MorphologyData')
                        else:
                            output = output._records_to_list(output.nodes)
                        self._busy = False
                        return (output, True)


                if isinstance(output, QueryWrapper):
                    #print  output._records_to_list(output.nodes)
                    if task['format'] == 'morphology':
                        #df = output.get_data(cls='MorphologyData')[0]
                        try:
                            all_data = output.get_data_qw(cls = 'MorphologyData')
                            c = (output + all_data).get_as(as_type = 'nx', edges = True)
                            output = {}
                            for node, data in c.nodes(data = True):
                                if data['class'] in ['Neuron', 'Synapse']:
                                    output[node] = data
                                    for n, d in c.out_edges(node):
                                        outV = c.nodes[d]
                                        if outV['class'] == 'MorphologyData':
                                            output[node]['MorphologyData'] = outV
                                            output[node]['MorphologyData']['rid'] = d
                        except KeyError:
                            output = {}

                    elif task['format'] == 'no_result':
                        output = {}
                    elif task['format'] == 'get_data':
                        if 'cls' in task:
                            output = output.get_data(cls=task['cls'])[0].to_dict(orient='index')
                        else:
                            output = output.get_data()[0].to_dict(orient='index')
                    elif task['format'] == 'nx':
                        nx_graph = output.get_as('nx')
                        #output = {'nodes': nx_graph.node, 'edges': nx_graph.edge}
                        output = {'nodes': dict(nx_graph.nodes(data=True)), 'edges': list(nx_graph.edges(data=True))}
                    elif task['format'] == 'nk':
                        output = output.traverse_owned_by_get_toplevel()
                        for x in output['LPU']:
                            g = output['LPU'][x].get_as('nx')
                            #output['LPU'][x] = {'nodes': g.node, 'edges': g.edge}
                            output['LPU'][x] = {'nodes': dict(g.nodes(data=True)), 'edges': list(g.edges(data=True))}
                        for x in output['Pattern']:
                            g = output['Pattern'][x].get_as('nx')
                            #output['Pattern'][x] = {'nodes': g.node, 'edges': g.edge}
                            output['Pattern'][x] = {'nodes': dict(g.nodes(data=True)), 'edges': list(g.edges(data=True))}


                    elif task['format'] == 'df':
                        dfs = output.get_as()
                        output = {}
                        if 'node_cols' in task:
                            output['nodes'] = dfs[0][task['node_cols']].to_dict(orient='index')
                        else:
                            output['nodes'] = dfs[0].to_dict(orient='index')
                        if 'edge_cols' in task:
                            output['edges'] = dfs[1][task['edge_cols']].to_dict(orient='index')
                        else:
                            output['edges'] = dfs[1].to_dict(orient='index')
                    elif task['format'] == 'qw':
                        pass
                # Default to nodes and edges df
                    else:
                        dfs = output.get_as()
                        output = {'nodes':dfs[0].to_dict(orient='index'),
                                  'edges': dfs[1].to_dict(orient='index')}
                else:
                    output = str(output)
                if threshold and isinstance(output, dict):
                    chunked_output = []
                    for c in chunks(output, threshold):
                        chunked_output.append(c)
                    output = chunked_output
                self._busy = False
                return (output, True)

            elif 'query' in task:
                succ = self.process_query(task)
                if succ:
                    if query_results:
                        task['command'] = {"retrieve":{"state":0}}
                        output = (None,)
                        try:
                            self._busy = False
                            output = self.receive_task(task, threshold)
                            if output[0]==None:
                                succ=False
                        except Exception as e:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                            print("An error occured during 'query':\n" + tb)
                            succ = False
                        self._busy = False
                        if 'temp' in task and task['temp'] and len(user.state)>=2:
                            user.process_command({'undo':{'states':1}})
                        return (output[0], succ)
                self._busy = False
                return None, succ
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            print("An error occured during 'receive_task':\n" + tb)
            self._busy = False
            return None, False


class query_processor():

    def __init__(self, graph, debug = False):
        self.class_list = {}
        self.graph = graph
        self.debug = debug
        self.load_class_list()

    def load_class_list(self):
        # Dynamically build acceptable methods from the registry
        # This could potentially be made stricter with a custom hardcoded subset
        for k in models.Node.registry:
            try:
                plural = eval('models.' + k + ".registry_plural")
                self.class_list[k]=eval("self.graph." + plural)
            except:
                print("Warning:Class %s left out of class list" % k)
                e = sys.exc_info()[0]
                print(e)
        #print self.class_list

    def process(self,query_list,user):
        """ take a query of the form
           [{'object':...:,'action...'}]
        """
        assert type(query_list) is list

        task_memory = []
        for q in query_list:
            # Assume each query must act on the previous result, is this valid?
            task_memory.append(self.process_single(q,user,task_memory))
        if not len(task_memory):
            user.append([])
            return []
        if 'temp' in query_list[-1] and query_list[-1]['temp']:
            return task_memory[-1]
        output = task_memory[-1]

        user.append(output)
        return output


    def process_single(self,query,user,task_memory):
        """  accetpt a single query object or form
           [{'object':...:,'action...'}]
        """
        assert 'object' in query and 'action' in query
        assert 'class' in query['object'] or 'state' in query['object'] or 'memory' in query['object'] or 'rid' in query['object']
        '''
        if 'class' in query['object']:
            # Retrieve Class
            class_name = query['object']['class']
            na_object = self.class_list[class_name]
            # convert result to a query wrapper to save
        '''
        if 'state' in query['object']:
            state_num = query['object']['state']

            if isinstance(state_num, int):
                state_num = int(state_num)

            assert isinstance(state_num, int)
            na_object = user.retrieve(index = state_num)

        elif 'memory' in query['object']:
            assert task_memory is not []
            memory_index = query['object']['memory']

            if isinstance(memory_index, int):
                memory_index = int(memory_index)

            assert isinstance(memory_index, int)
            assert len(task_memory) > memory_index
            na_object = task_memory[-1-memory_index]

        # Retrieve method
        if 'method' in query['action']: # A class query can only take a method.
            if 'class' in query['object']:
                method_call = query['action']['method']
                assert len(method_call.keys()) == 1
                method_name = list(method_call.keys())[0]

                method_args = method_call[method_name]
                columns = ""
                attrs = []
                for k, v in method_args.items():
                    if not(isinstance(v, list)):
                        if isinstance(v, (str, numbers.Number)):
                            v = [str(v)]
                    else:
                        # To prevent issues with unicode objects
                        if v and isinstance(v[0],str): v = [str(val) for val in v]
                    if len(v) == 1 and isinstance(v[0],str) and len(v[0])>=2 and str(v[0][:2]) == '/r':
                        attrs.append(""" %s matches "%s" """ % (str(k), str(v[0][2:])))
                    else:
                        attrs.append(""" %s in %s""" % (str(k), str(v)))
                attrs = " and ".join(attrs)
                if attrs: attrs = "where " + attrs
                query['object']['class'] = _list_repr(query['object']['class'])
                q = {}
                for i, a in enumerate(query['object']['class']):
                    var = '$q'+str(i)
                    q[var] = """ {var} = (select from {cls} {attrs})""".format(var=var,
                                                                          cls=str(a),
                                                                          attrs=str(attrs))
                query_str = """select from (select expand($a) let %s, $a = unionall(%s))""" % \
                    (", ".join(list(q.values())), ", ".join(list(q.keys())) )
                query_str = QueryString(query_str,'sql')
                query_result = QueryWrapper(self.graph, query_str, debug = self.debug)
            elif 'rid' in query['object']:
                if isinstance(query['object']['rid'], list):
                    query_result = QueryWrapper.from_rids(self.graph, *query['object']['rid'])
                elif isinstance(query['object']['rid'], str):
                    query_result = QueryWrapper.from_rids(self.graph, query['object']['rid'])
                else:
                    raise ValueError('rid must be either a list of rids or str')
            else:
                if na_object is None:
                    query_result = QueryWrapper.empty_query(self.graph, debug = self.debug)
                else:
                    method_call = query['action']['method']
                    assert len(method_call.keys()) == 1
                    method_name = list(method_call.keys())[0]
                    # check method is valid
                    assert method_name in dir(type(na_object))
                    # Retrieve arguments
                    method_args = byteify(method_call[method_name])

                    if 'pass_through' in method_args:
                        pass_through = method_args.pop('pass_through')
                        if isinstance(pass_through,list) and pass_through and isinstance(pass_through[0],list):
                            query_result = getattr(na_object, method_name)(*pass_through,**method_args)
                        else:
                            query_result = getattr(na_object, method_name)(pass_through,**method_args)
                    else:
                        query_result = getattr(na_object, method_name)(**method_args)
        elif 'op' in query['action']:
            if na_object is None:
                na_object = QueryWrapper.empty_query(self.graph, debug = self.debug)

            method_call = query['action']['op']
            assert len(method_call.keys()) == 1
            method_name = list(method_call.keys())[0]
            # WIP: Check which operators are supported
            # What if we want to be a op between two past states!
            # retieve past external state or internal memory state
            if 'state' in method_call[method_name]:
                state_num = method_call[method_name]['state']
                assert isinstance(state_num, int)
                past_object = user.retrieve(index = state_num)
            elif 'memory' in method_call[method_name]:
                assert task_memory is not []
                memory_index = method_call[method_name]['memory']

                if isinstance(memory_index, int):
                    memory_index = int(memory_index)

                assert isinstance(memory_index, int)
                assert len(task_memory) > memory_index
                past_object = task_memory[-1-memory_index]


            #query_result = getattr(na_object, method_name)(method_args)
            ## INVERSE THIS na and method argis (WHY?)
            query_result = getattr(na_object, method_name)(past_object)

        # convert result to a query wrapper to save
        if type(query_result) is not QueryWrapper:
            output = QueryWrapper.from_objs(self.graph,query_result.all(), debug = self.debug)
        else:
            output = query_result

        return output


class user_list():
    def __init__(self, state_limit=10, debug = False):
        self.list = {}
        self.state_limit = state_limit
        self.debug = debug
        pass

    def user(self, user_id, db, force_reconnect = False):
        if user_id not in self.list:
            st = state.State(user_id)
            self.list[user_id] = {'state': st,
                                  'server': neuroarch_server(db, user = st, debug = self.debug)}
        else:
            if force_reconnect:
                st = self.list[user_id]['state']
                self.list[user_id] = {'state': st,
                                      'server': neuroarch_server(
                                            db, user = st, debug = self.debug)}
        return self.list[user_id]

    def cleanup(self):
        cleansed = []
        for user in self.list:
            x = self.list[user]['state'].memory_management()
            if x:
                cleansed.append(user)
        for user in cleansed:
            del self.list[user]

        return cleansed


class AppSession(ApplicationSession):

    log = Logger()

    def onConnect(self):
        setProtocolOptions(self._transport,
                           maxFramePayloadSize = 0,
                           maxMessagePayloadSize = 0,
                           autoFragmentSize = 65536)
        if self.config.extra['auth']:
            self.join(self.config.realm, [u"wampcra"], user)
        else:
            self.join(self.config.realm, [], user)

    def onChallenge(self, challenge):
        if challenge.method == u"wampcra":
            #print("WAMP-CRA challenge received: {}".format(challenge))

            if u'salt' in challenge.extra:
                # salted secret
                key = auth.derive_key(secret,
                                      challenge.extra['salt'],
                                      challenge.extra['iterations'],
                                      challenge.extra['keylen'])
            else:
                # plain, unsalted secret
                key = secret

            # compute signature for challenge, using the key
            signature = auth.compute_wcs(key, challenge.extra['challenge'])

            # return the signature to the router for verification
            return signature

        else:
            raise Exception("Invalid authmethod {}".format(challenge.method))

    def na_query_on_end(self):
        self._current_concurrency -= 1
        self.log.info('na_query() ended ({invocations} invocations, current concurrency {current_concurrency} of max {max_concurrency})', invocations=self._invocations_served, current_concurrency=self._current_concurrency, max_concurrency=self._max_concurrency)

    def na_query_on_error(self):
        self._current_concurrency -= 1
        self.log.info('na_query() encountered error ({invocations} invocations, current concurrency {current_concurrency} of max {max_concurrency})', invocations=self._invocations_served, current_concurrency=self._current_concurrency, max_concurrency=self._max_concurrency)


    @inlineCallbacks
    def onJoin(self, details):
        self.server_config = {six.u('name'): six.u(self.config.extra['name']),
                              six.u('dataset'): six.u(self.config.extra['dataset']),
                              six.u('autobahn'): six.u(autobahn.__version__)}
        self.db_connection = na.NeuroArch(
                                self.config.extra['database'],
                                user = self.config.extra['username'],
                                password = self.config.extra['password'],
                                port = self.config.extra['port'],
                                mode = 'r')
        self.na_debug = self.config.extra['debug']
        self._max_concurrency = 10
        self._current_concurrency = 0
        self._invocations_served = 0
        self.user_list = user_list(debug = self.na_debug)

        arg_kws = ['color']

        reactor.suggestThreadPoolSize(self._max_concurrency*2)
        verb_translations = {'unhide': 'show',
                             'color': 'setcolor',
                             'keep' : 'remove',
                             'blink' : 'animate',
                             'unblink' : 'unanimate',
                             'remove': 'remove'}

        @inlineCallbacks
        def na_query(task,details=None):
            self._invocations_served += 1
            self._current_concurrency += 1

            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)

            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller

            if not 'format' in task: task['format'] = 'morphology'
            threshold = None
            if details.progress:
                threshold = task['threshold'] if 'threshold' in task else 20
            if 'verb' in task and task['verb'] not in ['add','show']: threshold=None
            if task['format'] != 'morphology': threshold=None

            self.log.info("na_query() called with task: {task} ,(current concurrency {current_concurrency} of max {max_concurrency})", current_concurrency=self._current_concurrency, max_concurrency=self._max_concurrency, task=task)

            server = self.user_list.user(
                                user_id,
                                self.db_connection)['server']
            (res, succ) = yield threads.deferToThread(server.receive_task, task, threshold)

            if not succ:
                print("attempt to restart connection to DB.")
                self.db_connection.reconnect()
                server = self.user_list.user(user_id, self.db_connection, force_reconnect = True)['server']
                (res, succ) = yield threads.deferToThread(server.receive_task, task, threshold)

            uri = 'ffbo.ui.receive_msg.%s' % user_id
            if not(type(uri)==six.text_type): uri = six.u(uri)
            cmd_uri = 'ffbo.ui.receive_cmd.%s' % user_id
            if not(type(cmd_uri)==six.text_type): cmd_uri = six.u(cmd_uri)

            try:
                if succ:
                    yield self.call(uri, {'info':{'success':
                                                  'Fetching results from NeuroArch'}})
                else:
                    yield self.call(uri, {'info':{'error':
                                                  'Error executing query on NeuroArch'}})
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                print("An error occured when calling 'receive_msg':\n" + tb)

            try:
                if(task['format'] == 'morphology' and (not 'verb' in task or task['verb'] == 'show')):
                    yield self.call(cmd_uri,
                                    {'commands': {'reset':''}})
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                print("An error occured during 'receive_cmd':\n" + tb)

            if('verb' in task and task['verb'] not in ['add','show']):
                try:
                    task['verb'] = verb_translations[task['verb']]
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                    print("An error occured during 'verb_translation':\n" + tb)
                    pass

                args = []
                if 'color' in task: task['color'] = '#' + task['color']
                for kw in arg_kws:
                    if kw in task: args.append(task[kw])
                if len(args)==1: args=args[0]
                yield self.call(cmd_uri, {'commands': {task['verb']: [res, args]}})
                self.na_query_on_end()
                returnValue({'info':{'success':'Finished processing command'}})
            else:
                if ('data_callback_uri' in task and 'queryID' in task):
                    if threshold:
                        for c in res:
                            yield self.call(six.u(task['data_callback_uri'] + '.%s' % details.caller),
                                            {'data': c, 'queryID': task['queryID']})
                    else:
                        yield self.call(six.u(task['data_callback_uri'] + '.%s' % details.caller),
                                            {'data': res, 'queryID': task['queryID']})
                    self.na_query_on_end()
                    returnValue({'info': {'success':'Finished fetching all results from database'}})
                else:
                    if details.progress and threshold:
                        for c in res:
                            details.progress(c)
                        self.na_query_on_end()
                        returnValue({'info': {'success':'Finished fetching all results from database'}})
                    else:
                        self.na_query_on_end()
                        returnValue({'info': {'success':'Finished fetching all results from database'},
                                     'data': res})
        uri = six.u( 'ffbo.na.query.%s' % str(details.session) )
        yield self.register(na_query, uri, RegisterOptions(details_arg='details',concurrency=self._max_concurrency//2))

        @inlineCallbacks
        def get_data_sub(q):
            res = list(q.get_as('nx', edges = False, deepcopy = False).nodes.values())[0]
            n_obj = q.nodes_as_objs[0]
            orid = n_obj._id
            # ds = [n for n in qq.in_('Owns') if isinstance(n, DataSource)]
            # if len(ds):
            #     res['data_source'] = [x.name for x in ds]
            # else:
            #     qq = q.get_data_qw()
            #     ds = [n for n in qq.in_('Owns') if isinstance(n, DataSource)]
            #     if len(ds):
            #         res['data_source'] = [x.name for x in ds]
            #     else:
            #         res['data_source'] = ['Unknown']
            ds = q.owned_by(cls='DataSource')
            if ds.nodes:
                res['data_source'] = {x.name: getattr(x, 'version', '') for x in ds.nodes}
            else:
                ds = q.get_data_qw().owned_by(cls='DataSource')
                if ds.nodes:
                    res['data_source'] = {x.name: getattr(x, 'version', '') for x in ds.nodes}
                else:
                    res['data_source'] = {'Unknown': ''}

            subdata = q.get_data(cls=['NeurotransmitterData', 'GeneticData'],as_type='nx',edges=False,deepcopy=False).nodes
            ignore = ['name','uname','label','class']
            key_map = {'Transmitters': 'transmitters'}#'transgenic_lines': 'Transgenic Lines'}
            for x in subdata.values():
                up_data = {(key_map[k] if k in key_map else k ):x[k] for k in x if k not in ignore}
                res.update(up_data)

            res['orid'] = orid
            try:
                res['info'] = n_obj.info;
            except:
                res['info'] = {}
            res = {'summary': res}
            if 'FlyCircuit' in res['summary']['data_source']:
                try:
                    flycircuit_data = yield self.call(six.u( 'ffbo.processor.fetch_flycircuit' ), res['summary']['name'])
                    res['summary']['flycircuit_data'] = flycircuit_data
                except:
                    pass

            arborization_data = q.get_data(cls='ArborizationData', as_type='nx',edges=False,deepcopy=False).nodes
            ignore = ['name','uname','label','class']
            up_data = {}

            for x in arborization_data.values():
                key_map = {k:k for k in x}
                if 'FlyCircuit' in res['summary']['data_source']:
                    key_map['dendrites'] = 'inferred_dendritic_segments'
                    key_map['axons'] = 'inferred_axonal_segments'
                else:
                    key_map['dendrites'] = 'input_synapses'
                    key_map['axons'] = 'output_synapses'
                up_data.update({key_map[k]:x[k] for k in x if k not in ignore})
            if up_data: res['summary']['arborization_data'] = up_data

            post_syn=q.gen_traversal_out(['SendsTo',['InferredSynapse', 'Synapse']],min_depth=1)
            pre_syn=q.gen_traversal_in(['SendsTo',['InferredSynapse', 'Synapse']],min_depth=1)

            post_data = []
            if self.na_debug:
                start = time.time()
            if len(post_syn.nodes):
                post_syn_dict = {}
                synapses = post_syn.get_as('nx', edges=False, deepcopy = False)
                synapse_rids = ','.join(list(synapses.nodes()))
                n_rec=q._graph.client.command("SELECT $path from (traverse out('SendsTo') FROM [{}] maxdepth 1)".format(synapse_rids))
                ntos = {n[1]:n[0] for n in [re.findall('\#\d+\:\d+', x.oRecordData['$path']) for x in n_rec] if len(n)==2}
                neuron_rids = list(set(ntos.keys()))
                neurons = QueryWrapper.from_rids(q._graph, *neuron_rids, debug = self.na_debug).get_as('nx', edges=False, deepcopy=False)

                post_rids = ','.join(list(neurons.nodes()))
                post_map_command = """select $path from (traverse out('HasData') from [{},{}] maxdepth 1) where @class='MorphologyData'""".format(post_rids,synapse_rids)
                post_map_l = {n[0]:n[1] for n in [re.findall('\#\d+\:\d+', x.oRecordData['$path']) for x in q._graph.client.command(post_map_command)] if len(n)==2}

                post_data = []
                for neu_id, syn_id in ntos.items():
                    info = {'has_morph': 0, 'has_syn_morph': 0}
                    info['number'] = synapses.nodes[syn_id].get('N', 1)
                    info['n_rid'] = neu_id
                    info['s_rid'] = syn_id
                    if neu_id in post_map_l:
                        info['has_morph'] = 1
                        info['rid'] = post_map_l[neu_id]
                    if syn_id in post_map_l:
                        info['has_syn_morph'] = 1
                        info['syn_rid'] = post_map_l[syn_id]
                        if 'uname' in synapses.nodes[syn_id]:
                            info['syn_uname'] = synapses.nodes[syn_id]['uname']
                    info['inferred'] = (synapses.nodes[syn_id]['class'] == 'InferredSynapse')
                    info.update(neurons.nodes[neu_id])
                    post_data.append(info)
                post_data = sorted(post_data, key=lambda x: x['number'])

            pre_data = []
            if len(pre_syn.nodes):
                pre_syn_dict = {}
                synapses = pre_syn.get_as('nx', edges=False, deepcopy = False)
                synapse_rids = ','.join(list(synapses.nodes()))
                n_rec=q._graph.client.command("""SELECT $path from (traverse in('SendsTo') FROM [{}] maxdepth 1)""".format(synapse_rids))
                ntos = {n[1]:n[0] for n in [re.findall('\#\d+\:\d+', x.oRecordData['$path']) for x in n_rec] if len(n)==2}
                neuron_rids = list(set(ntos.keys()))
                neurons = QueryWrapper.from_rids(q._graph, *neuron_rids, debug = self.na_debug).get_as('nx', edges=False, deepcopy=False)

                pre_rids = ','.join(list(neurons.nodes()))
                pre_map_command = """select $path from (traverse out('HasData') from [{},{}] maxdepth 1) where @class='MorphologyData'""".format(pre_rids, synapse_rids)
                pre_map_l = {n[0]:n[1] for n in [re.findall('\#\d+\:\d+', x.oRecordData['$path']) for x in q._graph.client.command(pre_map_command)] if len(n)==2}

                for neu_id, syn_id in ntos.items():
                    info = {'has_morph': 0, 'has_syn_morph': 0}
                    info['number'] = synapses.nodes[syn_id].get('N', 1)
                    info['n_rid'] = neu_id
                    info['s_rid'] = syn_id
                    if neu_id in pre_map_l:
                        info['has_morph'] = 1
                        info['rid'] = pre_map_l[neu_id]
                    if syn_id in pre_map_l:
                        info['has_syn_morph'] = 1
                        info['syn_rid'] = pre_map_l[syn_id]
                        if 'uname' in synapses.nodes[syn_id]:
                            info['syn_uname'] = synapses.nodes[syn_id]['uname']
                    info['inferred'] = (synapses.nodes[syn_id]['class'] == 'InferredSynapse')
                    info.update(neurons.nodes[neu_id])
                    pre_data.append(info)
                pre_data = sorted(pre_data, key=lambda x: x['number'])

            # OLDER IMPLEMENTATION
            # post_syn_q = q.gen_traversal_out(['SendsTo',['InferredSynapse', 'Synapse']],['SendsTo','Neuron'],min_depth=1)
            # pre_syn_q = q.gen_traversal_in(['SendsTo',['InferredSynapse', 'Synapse']],['SendsTo','Neuron'],min_depth=1)
            # post_syn_q.execute_edges(edge_class = 'SendsTo')
            # post_syn = post_syn_q.get_as('nx', deepcopy = False)
            # pre_syn_q.execute_edges(edge_class = 'SendsTo')
            # pre_syn = pre_syn_q.get_as('nx', deepcopy = False)
            # if post_syn.nodes() or pre_syn.nodes():
            #     post_rids = str(post_syn.nodes()).replace("'","")
            #     pre_rids = str(pre_syn.nodes()).replace("'","")
            #
            #     post_map_command = "select $path from (traverse out('HasData') from %s while $depth<=1) where @class='MorphologyData'" % post_rids
            #     pre_map_command = "select $path from (traverse out('HasData') from %s while $depth<=1) where @class='MorphologyData'" % pre_rids
            #
            #     post_map_l = [x.oRecordData['$path'] for x in q._graph.client.command(post_map_command)]
            #     pre_map_l = [x.oRecordData['$path'] for x in q._graph.client.command(pre_map_command)]
            #
            #     post_map = {}
            #     pre_map = {}
            #
            #     for p in post_map_l:
            #         m = re.findall('\#\d+\:\d+', p)
            #         if len(m)==2:
            #             post_map[m[0]] = m[1]
            #
            #     for p in pre_map_l:
            #         m = re.findall('\#\d+\:\d+', p)
            #         if len(m)==2:
            #             pre_map[m[0]] = m[1]
            #
            #     post_data = []
            #     for (syn, neu) in post_syn.edges():
            #         if not (post_syn.node[syn]['class']  == 'InferredSynapse' or
            #                 post_syn.node[syn]['class']  == 'Synapse'):
            #             continue
            #         info = {'has_morph': 0, 'has_syn_morph': 0}
            #         if 'N' not in post_syn.node[syn]:
            #             print post_syn.node[syn]
            #             info['number'] = 1
            #         else:
            #             info['number'] =  post_syn.node[syn]['N']
            #         if neu in post_map:
            #             info['has_morph'] = 1
            #             info['rid'] = post_map[neu]
            #         if syn in post_map:
            #             info['has_syn_morph'] = 1
            #             info['syn_rid'] = post_map[syn]
            #             if 'uname' in post_syn.node[syn]:
            #                 info['syn_uname'] = post_syn.node[syn]['uname']
            #         info['inferred'] = (post_syn.node[syn]['class'] == 'InferredSynapse')
            #         info.update(post_syn.node[neu])
            #         post_data.append(info)
            #
            #     post_data = sorted(post_data, key=lambda x: x['number'])
            #
            #     pre_data = []
            #     for (neu, syn) in pre_syn.edges():
            #         if not (pre_syn.node[syn]['class']  == 'InferredSynapse' or
            #                 pre_syn.node[syn]['class']  == 'Synapse'):
            #             continue
            #         info = {'has_morph': 0, 'has_syn_morph': 0}
            #         if 'N' not in pre_syn.node[syn]:
            #             print pre_syn.node[syn]
            #             info['number'] = 1
            #         else:
            #             info['number'] =  pre_syn.node[syn]['N']
            #         if neu in pre_map:
            #             info['has_morph'] = 1
            #             info['rid'] = pre_map[neu]
            #         if syn in pre_map:
            #             info['has_syn_morph'] = 1
            #             info['syn_rid'] = pre_map[syn]
            #             if 'uname' in pre_syn.node[syn]:
            #                 info['syn_uname'] = pre_syn.node[syn]['uname']
            #         info['inferred'] = (pre_syn.node[syn]['class'] == 'InferredSynapse')
            #         info.update(pre_syn.node[neu])
            #         pre_data.append(info)
            #     pre_data = sorted(pre_data, key=lambda x: x['number'])

                # Summary PreSyn Information
                pre_sum = {}
                for x in pre_data:
                    cls = x['name'].split('-')[0]
                    try:
                        if cls=='5': cls = x['name'].split('-')[:2].join('-')
                    except Exception as e:
                        pass
                    if cls in pre_sum: pre_sum[cls] += x['number']
                    else: pre_sum[cls] = x['number']
                pre_N =  np.sum(list(pre_sum.values()))
                pre_sum = {k: 100*float(v)/pre_N for (k,v) in pre_sum.items()}

                # Summary PostSyn Information
                post_sum = {}
                for x in post_data:
                    cls = x['name'].split('-')[0]
                    if cls in post_sum: post_sum[cls] += x['number']
                    else: post_sum[cls] = x['number']
                post_N =  np.sum(list(post_sum.values()))
                post_sum = {k: 100*float(v)/post_N for (k,v) in post_sum.items()}

                res.update({
                    'connectivity':{
                        'post': {
                            'details': post_data,
                            'summary': {
                                'number': int(post_N),
                                'profile': post_sum
                            }
                        }, 'pre': {
                            'details': pre_data,
                            'summary': {
                                'number': int(pre_N),
                                'profile': pre_sum
                            }
                        }
                    }
                })
            if self.na_debug:
                print("Finished 'get_data connectivity' in", time.time()-start)
            returnValue({'data':res})

        def is_rid(rid):
            if isinstance(rid, str) and re.search('^\#\d+\:\d+$', rid):
                return True
            else:
                return False

        def get_syn_data_sub(q):
            res = list(q.get_as('nx', edges = False, deepcopy = False).nodes.values())[0]
            synapse = q.nodes_as_objs[0]
            syn_id = synapse._id
            res['orid'] = syn_id
            ds = q.owned_by(cls='DataSource')
            if ds.nodes:
                res['data_source'] = {x.name: getattr(x, 'version', '') for x in ds.nodes}
            else:
                ds = q.get_data_qw().owned_by(cls='DataSource')
                if ds.nodes:
                    res['data_source'] = {x.name: getattr(x, 'version', '') for x in ds.nodes}
                else:
                    res['data_source'] = {'Unknown': ''}

            subdata = q.get_data(cls = ['NeurotransmitterData', 'GeneticData', 'MorphologyData'],
                                 as_type = 'nx', edges = False, deepcopy = False).nodes
            ignore = ['name','uname','label','class', 'x', 'y', 'z', 'r', 'parent', 'identifier', 'sample', 'morph_type', 'confidence']
            key_map = {'Transmitters': 'transmitters', 'N': 'number'}#'transgenic_lines': 'Transgenic Lines'}
            for x in subdata.values():
                up_data = {(key_map[k] if k in key_map else k ):x[k] for k in x if k not in ignore}
                res.update(up_data)
            for x in res:
                if x in key_map:
                    res[key_map[x]] = res[x]
                    del res[x]
            if 'region' in res:
                res['synapse_locations'] = Counter(res['region'])
                del res['region']

            post_neuron = synapse.out('SendsTo')[0]
            pre_neuron = synapse.in_('SendsTo')[0]

            post_neuron_morph = [n for n in post_neuron.out('HasData') if isinstance(n, models.MorphologyData)]
            pre_neuron_morph = [n for n in pre_neuron.out('HasData') if isinstance(n, models.MorphologyData)]

            post_data = []
            neu_id = post_neuron._id
            post_neuron = QueryWrapper.from_rids(q._graph, neu_id, debug = self.na_debug).get_as('nx', edges=False, deepcopy=False)
            info = {'has_morph': 0, 'has_syn_morph': 0}
            info['number'] = getattr(synapse, 'N', 1)
            info['n_rid'] = neu_id
            if len(post_neuron_morph):
                info['has_morph'] = 1
                info['rid'] = post_neuron_morph[0]._id
            info.update(post_neuron.nodes[neu_id])
            post_data.append(info)

            pre_data = []
            neu_id = pre_neuron._id
            pre_neuron = QueryWrapper.from_rids(q._graph, neu_id, debug = self.na_debug).get_as('nx', edges=False, deepcopy=False)
            info = {'has_morph': 0, 'has_syn_morph': 0}
            info['number'] = getattr(synapse, 'N', 1)
            info['n_rid'] = neu_id
            if len(pre_neuron_morph):
                info['has_morph'] = 1
                info['rid'] = pre_neuron_morph[0]._id
            info.update(pre_neuron.nodes[neu_id])
            pre_data.append(info)

            res = {'data':{'summary': res,
                           'connectivity':{
                               'post': {
                                   'details': post_data,
                               }, 'pre': {
                                   'details': pre_data,
                               }}
                   }}
            return res


        @inlineCallbacks
        def na_get_data(task,details=None):
            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)

            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            threshold = None

            self.log.info("na_get_data() called with task: {task}",task=task)
            server = self.user_list.user(
                            user_id,
                            self.db_connection)['server']
            try:
                if not is_rid(task['id']):
                    returnValue({})
                elem = server.graph.get_element(task['id'])
                q = QueryWrapper.from_objs(server.graph,[elem], self.na_debug)
                callback = get_data_sub if elem.element_type == 'Neuron' else get_syn_data_sub
                if not (elem.element_type == 'Neuron' or elem.element_type == 'Synapse' or elem.element_type=='InferredSynapse'):
                    qn = q.gen_traversal_in(['HasData','Neuron'],min_depth=1)
                    if not qn.nodes:
                        q = q.gen_traversal_in(['HasData',['Synapse', 'InferredSynapse']],min_depth=1)
                        if not q.nodes:
                            raise ValueError('Did not find the Synapse node')
                    else:
                        q = qn
                        callback = get_data_sub
                #res = yield threads.deferToThread(get_data_sub, q)
                res = yield callback(q)
            except (PyOrientConnectionException, ValueError) as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                print("An error occured during 'na_get_data':\n" + tb)
                print("attempt to restart connection to DB.")
                self.db_connection.reconnect()
                server = self.user_list.user(user_id, self.db_connection, force_reconnect = True)['server']
                print('success')
                try:
                    if not is_rid(task['id']):
                        returnValue({})
                    elem = server.graph.get_element(task['id'])
                    q = QueryWrapper.from_objs(server.graph,[elem], self.na_debug)
                    callback = get_data_sub if elem.element_type == 'Neuron' else get_syn_data_sub
                    if not (elem.element_type == 'Neuron' or elem.element_type == 'Synapse' or elem.element_type=='InferredSynapse'):
                        qn = q.gen_traversal_in(['HasData','Neuron'],min_depth=1)
                        if not qn.nodes:
                            q = q.gen_traversal_in(['HasData',['Synapse', 'InferredSynapse']],min_depth=1)
                            if not q.nodes:
                                raise ValueError('Did not find the Synapse node')
                        else:
                            q = qn
                            callback = get_data_sub
                    #res = yield threads.deferToThread(get_data_sub, q)
                    res = yield callback(q)
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                    print("An error occured during 'na_get_data':\n" + tb)
                    self.log.failure("Error Retrieveing Data")
                    res = {}
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                print("An error occured during 'na_get_data':\n" + tb)
                self.log.failure("Error Retrieveing Data")
                res = {}
            returnValue(res)

        uri = six.u( 'ffbo.na.get_data.%s' % str(details.session) )
        yield self.register(na_get_data, uri, RegisterOptions(details_arg='details',concurrency=1))

        # These users can mark a tag as feautured or assign a tag to a festured list
        approved_featured_tag_creators = []

        def create_tag(task, details=None):
            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)
            if not "tag" in task:
                if "name" in task:
                    task["tag"] = task["name"]
                    del task["name"]
                else:
                    return {"info":{"error":
                                    "tag/name field must be provided"}}

            if ('FFBOdata' in task and
                details.caller_authrole == 'user' and
                details.caller_authid not in approved_featured_tag_creators):
                del task['FFBOdata']
            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            self.log.info("create_tag() called with task: {task} ",task=task)

            server = self.user_list.user(
                        user_id,
                        self.db_connection)['server']
            (output,succ) = server.receive_task({"command":{"retrieve":{"state":0}},"format":"qw"})
            if not succ:
                return {"info":{"error":
                                "There was an error creating the tag"}}
            if isinstance(output, QueryWrapper):
                if 'metadata' in task:
                    succ = output.tag_query_result_node(tag=task['tag'],
                                                        permanent_flag=True,
                                                        **task['metadata'])
                else:
                    succ = output.tag_query_result_node(tag=task['tag'],
                                                        permanent_flag=True)
                if succ==-1:
                    return {"info":{"error":"The tag already exists. Please choose a different one"}}
                else:
                    return {"info":{"success":"tag created successfully"}}

            else:
                return {"info":{"error":
                                "No data found in current workspace to create tag"}}
        uri = six.u( 'ffbo.na.create_tag.%s' % str(details.session) )
        yield self.register(create_tag, uri, RegisterOptions(details_arg='details',concurrency=1))

        def retrieve_tag(task,details=None):
            if not "tag" in task:
                return {"info":{"error":
                                "Tag must be provided"}}

            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)

            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            self.log.info("retrieve_tag() called with task: {task} ",task=task)

            server = self.user_list.user(
                            user_id,
                            self.db_connection)['server']
            tagged_result = QueryWrapper.from_tag(graph=server.graph, tag=task['tag'], debug = self.na_debug)
            if tagged_result and tagged_result['metadata'] and tagged_result['metadata']!='{}':
                server.user.append(tagged_result['qw'])
                return {'data':tagged_result['metadata'],
                        'info':{'success':'Server Retrieved Tag Succesfully'}}
            else:
                return {"info":{"error":
                                "No such tag exists in this database server"}}

        uri = six.u( 'ffbo.na.retrieve_tag.%s' % str(details.session) )
        yield self.register(retrieve_tag, uri, RegisterOptions(details_arg='details',concurrency=1))

        # Register a function to retrieve a single neuron information
        def retrieve_neuron(nid):
            self.log.info("retrieve_neuron() called with neuron id: {nid} ", nid = nid)
            res = server.retrieve_neuron(nid)
            print("retrieve neuron result: " + str(res))
            return res

        uri = six.u( 'ffbo.na.retrieve_neuron.%s' % str(details.session) )
        yield self.register(retrieve_neuron, uri,RegisterOptions(concurrency=self._max_concurrency//2))
        print("registered %s" % uri)


        # Listen for ffbo.processor.connected
        @inlineCallbacks
        def register_component():
            self.log.info( "Registering a component")
            # CALL server registration
            try:
                # registered the procedure we would like to call
                res = yield self.call(six.u( 'ffbo.server.register' ),details.session, six.u('na'),self.server_config)
                self.log.info("register new server called with result: {result}",
                                                    result=res)

            except ApplicationError as e:
                if e.error != 'wamp.error.no_such_procedure':
                    raise e

        yield self.subscribe(register_component, six.u( 'ffbo.processor.connected' ))
        self.log.info("subscribed to topic 'ffbo.processor.connected'")

        # Register for memory management pings
        @inlineCallbacks
        def memory_management():
            clensed_users = yield self.user_list.cleanup()
            self.log.info("Memory Manager removed users: {users}", users=clensed_users)
            for user in clensed_users:
                try:
                    yield self.publish(six.u( "ffbo.ui.update.%s" % user ), "Inactivity Detected, State Memory has been cleared")
                except Exception as e:
                    self.log.warn("Failed to alert user {user} or State Memory removal, with error {e}",user=user,e=e)

        yield self.subscribe(memory_management, six.u( 'ffbo.processor.memory_manager' ))
        self.log.info("subscribed to topic 'ffbo.processor.memory_management'")



        register_component()

def setProtocolOptions(transport,
                       version=None,
                       utf8validateIncoming=None,
                       acceptMaskedServerFrames=None,
                       maskClientFrames=None,
                       applyMask=None,
                       maxFramePayloadSize=None,
                       maxMessagePayloadSize=None,
                       autoFragmentSize=None,
                       failByDrop=None,
                       echoCloseCodeReason=None,
                       serverConnectionDropTimeout=None,
                       openHandshakeTimeout=None,
                       closeHandshakeTimeout=None,
                       tcpNoDelay=None,
                       perMessageCompressionOffers=None,
                       perMessageCompressionAccept=None,
                       autoPingInterval=None,
                       autoPingTimeout=None,
                       autoPingSize=None):
    """ from autobahn.websocket.protocol.WebSocketClientFactory.setProtocolOptions """

    transport.factory.setProtocolOptions(
            version = version,
            utf8validateIncoming = utf8validateIncoming,
            acceptMaskedServerFrames = acceptMaskedServerFrames,
            maskClientFrames = maskClientFrames,
            applyMask = applyMask,
            maxFramePayloadSize = maxFramePayloadSize,
            maxMessagePayloadSize = maxMessagePayloadSize,
            autoFragmentSize = autoFragmentSize,
            failByDrop = failByDrop,
            echoCloseCodeReason = echoCloseCodeReason,
            serverConnectionDropTimeout = serverConnectionDropTimeout,
            openHandshakeTimeout = openHandshakeTimeout,
            closeHandshakeTimeout = closeHandshakeTimeout,
            tcpNoDelay = tcpNoDelay,
            perMessageCompressionOffers = perMessageCompressionOffers,
            perMessageCompressionAccept = perMessageCompressionAccept,
            autoPingInterval = autoPingInterval,
            autoPingTimeout = autoPingTimeout,
            autoPingSize = autoPingSize)

    if version is not None:
        if version not in WebSocketProtocol.SUPPORTED_SPEC_VERSIONS:
            raise Exception("invalid WebSocket draft version %s (allowed values: %s)" % (version, str(WebSocketProtocol.SUPPORTED_SPEC_VERSIONS)))
        if version != transport.version:
            transport.version = version

    if utf8validateIncoming is not None and utf8validateIncoming != transport.utf8validateIncoming:
        transport.utf8validateIncoming = utf8validateIncoming

    if acceptMaskedServerFrames is not None and acceptMaskedServerFrames != transport.acceptMaskedServerFrames:
        transport.acceptMaskedServerFrames = acceptMaskedServerFrames

    if maskClientFrames is not None and maskClientFrames != transport.maskClientFrames:
        transport.maskClientFrames = maskClientFrames

    if applyMask is not None and applyMask != transport.applyMask:
        transport.applyMask = applyMask

    if maxFramePayloadSize is not None and maxFramePayloadSize != transport.maxFramePayloadSize:
        transport.maxFramePayloadSize = maxFramePayloadSize

    if maxMessagePayloadSize is not None and maxMessagePayloadSize != transport.maxMessagePayloadSize:
        transport.maxMessagePayloadSize = maxMessagePayloadSize

    if autoFragmentSize is not None and autoFragmentSize != transport.autoFragmentSize:
        transport.autoFragmentSize = autoFragmentSize

    if failByDrop is not None and failByDrop != transport.failByDrop:
        transport.failByDrop = failByDrop

    if echoCloseCodeReason is not None and echoCloseCodeReason != transport.echoCloseCodeReason:
        transport.echoCloseCodeReason = echoCloseCodeReason

    if serverConnectionDropTimeout is not None and serverConnectionDropTimeout != transport.serverConnectionDropTimeout:
        transport.serverConnectionDropTimeout = serverConnectionDropTimeout

    if openHandshakeTimeout is not None and openHandshakeTimeout != transport.openHandshakeTimeout:
        transport.openHandshakeTimeout = openHandshakeTimeout

    if closeHandshakeTimeout is not None and closeHandshakeTimeout != transport.closeHandshakeTimeout:
        transport.closeHandshakeTimeout = closeHandshakeTimeout

    if tcpNoDelay is not None and tcpNoDelay != transport.tcpNoDelay:
        transport.tcpNoDelay = tcpNoDelay

    if perMessageCompressionOffers is not None and pickle.dumps(perMessageCompressionOffers) != pickle.dumps(transport.perMessageCompressionOffers):
        if type(perMessageCompressionOffers) == list:
            #
            # FIXME: more rigorous verification of passed argument
            #
            transport.perMessageCompressionOffers = copy.deepcopy(perMessageCompressionOffers)
        else:
            raise Exception("invalid type %s for perMessageCompressionOffers - expected list" % type(perMessageCompressionOffers))

    if perMessageCompressionAccept is not None and perMessageCompressionAccept != transport.perMessageCompressionAccept:
        transport.perMessageCompressionAccept = perMessageCompressionAccept

    if autoPingInterval is not None and autoPingInterval != transport.autoPingInterval:
        transport.autoPingInterval = autoPingInterval

    if autoPingTimeout is not None and autoPingTimeout != transport.autoPingTimeout:
        transport.autoPingTimeout = autoPingTimeout

    if autoPingSize is not None and autoPingSize != transport.autoPingSize:
        assert(type(autoPingSize) == float or type(autoPingSize) == int)
        assert(4 <= autoPingSize <= 125)
        transport.autoPingSize = autoPingSize



if __name__ == '__main__':
    from twisted.internet._sslverify import OpenSSLCertificateAuthorities
    from twisted.internet.ssl import CertificateOptions
    import OpenSSL.crypto
    import getpass

    # parse command line parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output.')
    parser.add_argument('--url', dest='url', type=six.text_type, default=url,
                        help='The router URL (defaults to value from config.ini)')
    parser.add_argument('--realm', dest='realm', type=six.text_type, default=realm,
                        help='The realm to join (defaults to value from config.ini).')
    parser.add_argument('--ca_cert', dest='ca_cert_file', type=six.text_type,
                        default=ca_cert_file,
                        help='Root CA PEM certificate file (defaults to value from config.ini).')
    parser.add_argument('--int_cert', dest='intermediate_cert_file', type=six.text_type,
                        default=intermediate_cert_file,
                        help='Intermediate PEM certificate file (defaults to value from config.ini).')
    parser.add_argument('--no-ssl', dest='ssl', action='store_false')
    parser.add_argument('--database', dest='db', type=six.text_type, default='na_server',
                        help='Orientdb database folder name.')
    parser.add_argument('--username', dest='user', type=six.text_type, default='root',
                        help='User name in orientdb database.')
    parser.add_argument('--password', dest='password', action='store_true',
                        help='Allow password prompt for authenticate database access.')
    parser.add_argument('--name', dest='name', type=six.text_type, default=None,
                        help='name of server, default to the same database name')
    parser.add_argument('--dataset', dest='dataset', type=six.text_type, default=None,
                        help='name of dataset, default to the same database name')
    parser.add_argument('--port', dest='port', type=int, default=2424,
                        help='binary port of orientdb, default to 2424')
    parser.set_defaults(ssl=ssl)
    parser.set_defaults(debug=debug)

    args = parser.parse_args()

    if args.password:
        pw = getpass.getpass()
    else:
        pw = 'root'

    # start logging
    if args.debug:
        txaio.start_logging(level='debug')
    else:
        txaio.start_logging(level='info')

    if args.dataset is None:
        dataset = args.database
    else:
        dataset = args.dataset
    if args.name is None:
        name = args.database
    else:
        name = args.name
    # any extra info we want to forward to our ClientSession (in self.config.extra)
    extra = {'auth': True, 'database': args.db, 'username': args.user,
             'password': pw, 'port': args.port, 'debug': args.debug,
             'dataset': dataset, 'name': name}

    if args.ssl:
        st_cert=open(args.ca_cert_file, 'rt').read()
        c=OpenSSL.crypto
        ca_cert=c.load_certificate(c.FILETYPE_PEM, st_cert)

        st_cert=open(args.intermediate_cert_file, 'rt').read()
        intermediate_cert=c.load_certificate(c.FILETYPE_PEM, st_cert)

        certs = OpenSSLCertificateAuthorities([ca_cert, intermediate_cert])
        ssl_con = CertificateOptions(trustRoot=certs)

        # now actually run a WAMP client using our session class ClientSession
        runner = ApplicationRunner(url=args.url, realm=args.realm, extra=extra, ssl=ssl_con)

    else:
        # now actually run a WAMP client using our session class ClientSession
        runner = ApplicationRunner(url=args.url, realm=args.realm, extra=extra)

    runner.run(AppSession, auto_reconnect=True)
