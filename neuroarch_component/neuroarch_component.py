import sys
import re

from math import isnan

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.logger import Logger

from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.exception import ApplicationError
from autobahn.wamp.types import RegisterOptions

from operator import itemgetter

import os
import argparse
import six
import txaio

import time

# Neuroarch Imports
import argparse

import numpy as np
import simplejson as json

from pyorient.ogm import Graph, Config
import pyorient.ogm.graph

import numpy as np

import time

from collections import Counter


from configparser import ConfigParser

# Grab configuration from file
root = os.path.expanduser("/")
home = os.path.expanduser("~")
filepath = os.path.dirname(os.path.abspath(__file__))
default_config = os.path.join(home, "config", "ffbo.neuroarch_component.ini")
backup_config1 = os.path.join(root, "config", "ffbo.neuroarch_component.ini")
backup_config2 = os.path.join(filepath, "..", "config.ini")
config = ConfigParser()
if os.path.exists(default_config):
    config.read(default_config)
elif os.path.exists(backup_config1):
    config.read(backup_config1)
elif os.path.exists(backup_config2):
    config.read(backup_config2)
else:
    raise Exception("No config file exists for this component")

user = config["UserInfo"]["user"]
secret = config["UserInfo"]["secret"]
ssl = eval(config["ServerInfo"]["ssl"])
url = config["ServerInfo"]["url"]
realm = config["ServerInfo"]["realm"]
authentication = eval(config["ServerInfo"]["authentication"])
debug = eval(config["DebugInfo"]["debug"])
ca_cert_file = config["CertInfo"]["ca_cert_file"]
intermediate_cert_file = config["CertInfo"]["intermediate_cert_file"]

# Required to handle dill's inability to serialize namedtuple class generator:
setattr(pyorient.ogm.graph, 'orientdb_version',
        pyorient.ogm.graph.ServerVersion)


from neuroarch.models import *
from neuroarch.query import QueryWrapper, QueryString, _list_repr

from autobahn.wamp import auth

# User access
import state

from pyorient.serializations import OrientSerialization

import uuid

from twisted.internet import reactor, threads
from itertools import islice


def byteify(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                                for key, value in input.iteritems()}
    elif isinstance(input, list):
                return [byteify(element) for element in input]
    elif isinstance(input, unicode):
                return input.encode('utf-8')
    else:
        return input

def chunks(data, SIZE=1000):
    it = iter(data)
    for i in xrange(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

class neuroarch_server(object):
    """ Methods to process neuroarch json tasks """

    def __init__(self,database='/na_server',username='root',password='root', user=None):
        try:
            self.graph = Graph(Config.from_url(database, username, password, initial_drop=False,serialization_type=OrientSerialization.Binary))
        except:
            #print "WARNING: Serialisation flag ignored"
            self.graph = Graph(Config.from_url(database, username, password, initial_drop=False))
        self.graph.include(Node.registry)
        self.graph.include(Relationship.registry)
        self.user = user
        self.query_processor = query_processor(self.graph)
        self._busy = False

    def retrieve_neuron(self,nid):
        # WIP: Currently retrieves all information for the get_as method, this will be refined when we know what data we want to store and pull out here
        try:
            n = self.graph.get_element(nid)
            if n == None:
                return {}
            else:
                output = QueryWrapper.from_objs(self.graph,[n])
                return output.get_as()[0].to_json()
        except Exception as e:
            raise e



    def process_query(self,task):
        """ configure a task processing, and format the results as desired """
        # WIP: Expand type of information that can be retrieved
        assert 'query'in task.keys()
        try:
            self.query_processor.process(task['query'],self.user)
            return True
        except Exception as e:
            print e
            return False

    @staticmethod
    def process_verb(output, user, verb):
        if verb == 'add':
            assert(len(user.state)>=2)
            user.state[-1] = output+user.state[-2]
        elif verb == 'keep':
            assert(len(user.state)>=2)
            user.state[-1] = output & user.state[-2]
            output = user.state[-2] - user.state[-1]
        elif verb == 'remove':
            assert(len(user.state)>=2)
            user.state[-1] = user.state[-2] - output
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
                        print e
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
                            #output= df[['sample','identifier','x','y','z','r','parent','name']].to_dict(orient='index')
                            #output= df.to_dict(orient='index')
                            output = output.get_data(cls='MorphologyData', as_type='nx').node
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
                        output = {'nodes': nx_graph.node, 'edges': nx_graph.edge}
                    elif task['format'] == 'nk':
                        output = output.traverse_owned_by_get_toplevel()
                        for x in output['LPU']:
                            g = output['LPU'][x].get_as('nx')
                            output['LPU'][x] = {'nodes': g.node, 'edges': g.edge}
                        for x in output['Pattern']:
                            g = output['Pattern'][x].get_as('nx')
                            output['Pattern'][x] = {'nodes': g.node, 'edges': g.edge}


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
                if query_results:
                    task['command'] = {"retrieve":{"state":0}}
                    output = (None,)
                    try:
                        self._busy = False
                        output = self.receive_task(task, threshold)
                        if output[0]==None:
                            succ=False
                    except Exception as e:
                        print e
                        succ = False
                    self._busy = False
                    if 'temp' in task and task['temp'] and len(user.state)>=2:
                        user.process_command({'undo':{'states':1}})
                    return (output[0], succ)
                self._busy = False
                return succ
        except Exception as e:
            print e
            self._busy = False

class query_processor():

    def __init__(self, graph):
        self.class_list = {}
        self.graph = graph
        self.load_class_list()

    def load_class_list(self):
        # Dynamically build acceptable methods from the registry
        # This could potentially be made stricter with a custom hardcoded subset
        for k in Node.registry:
            try:
                plural = eval(k + ".registry_plural")
                self.class_list[k]=eval("self.graph." + plural)
            except:
                print "Warning:Class %s left out of class list" % k
                e = sys.exc_info()[0]
                print e
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
        assert 'class' in query['object'] or 'state' in query['object'] or 'memory' in query['object']
        '''
        if 'class' in query['object']:
            # Retrieve Class
            class_name = query['object']['class']
            na_object = self.class_list[class_name]
            # convert result to a query wrapper to save
        '''
        if 'state' in query['object']:
            state_num = query['object']['state']

            if type(state_num) is long:
                state_num = int(state_num)

            assert type(state_num) in [int,long]
            na_object = user.retrieve(index = state_num)

        elif 'memory' in query['object']:
            assert task_memory is not []
            memory_index = query['object']['memory']

            if type(memory_index) is long:
                memory_index = int(memory_index)

            assert type(memory_index) is int
            assert len(task_memory) > memory_index
            na_object = task_memory[-1-memory_index]

        # Retrieve method
        if 'method' in query['action']: # A class query can only take a method.
            if 'class' in query['object']:
                method_call = query['action']['method']
                assert len(method_call.keys()) == 1
                method_name = method_call.keys()[0]

                method_args = method_call[method_name]
                columns = ""
                attrs = []
                for k, v in method_args.iteritems():
                    if not(isinstance(v, list)):
                        if isinstance(v, (basestring, numbers.Number)):
                            v = [str(v)]
                    else:
                        # To prevent issues with unicode objects
                        if v and isinstance(v[0],basestring): v = [str(val) for val in v]
                    if len(v) == 1 and isinstance(v[0],(unicode,str)) and len(v[0])>=2 and str(v[0][:2]) == '/r':
                        attrs.append("%s matches '%s'" % (str(k), str(v[0][2:])))
                    else:
                        attrs.append("%s in %s" % (str(k), str(v)))
                attrs = " and ".join(attrs)
                if attrs: attrs = "where " + attrs
                query['object']['class'] = _list_repr(query['object']['class'])
                q = {}
                for i, a in enumerate(query['object']['class']):
                    var = '$q'+str(i)
                    q[var] = "{var} = (select from {cls} {attrs})".format(var=var,
                                                                          cls=str(a),
                                                                          attrs=str(attrs))
                query_str = "select from (select expand($a) let %s, $a = unionall(%s))" % \
                    (", ".join(q.values()), ", ".join(q.keys()) )
                query_str = QueryString(query_str,'sql')
                query_result = QueryWrapper(self.graph, query_str)
            else:
                method_call = query['action']['method']


                assert len(method_call.keys()) == 1
                method_name = method_call.keys()[0]
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
            method_call = query['action']['op']
            assert len(method_call.keys()) == 1
            method_name = method_call.keys()[0]
            # WIP: Check which operators are supported
            # What if we want to be a op between two past states!
            # retieve past external state or internal memory state
            if 'state' in method_call[method_name]:
                state_num = method_call[method_name]['state']
                assert type(state_num) in [int,long]
                past_object = user.retrieve(index = state_num)
            elif 'memory' in method_call[method_name]:
                assert task_memory is not []
                memory_index = method_call[method_name]['memory']

                if type(memory_index) is long:
                    memory_index = int(memory_index)

                assert type(memory_index) is int
                assert len(task_memory) > memory_index
                past_object = task_memory[-1-memory_index]


            #query_result = getattr(na_object, method_name)(method_args)
            ## INVERSE THIS na and method argis (WHY?)
            query_result = getattr(na_object, method_name)(past_object)

        # convert result to a query wrapper to save
        if type(query_result) is not QueryWrapper:
            output = QueryWrapper.from_objs(self.graph,query_result.all())
        else:
            output = query_result

        return output


class user_list():
    def __init__(self,state_limit=10):
        self.list = {}
        self.state_limit = state_limit
        pass

    def user(self,user_id, database='/na_server',username='root',password='root'):
        if user_id not in self.list:
            st = state.State(user_id)
            self.list[user_id] = {'state': st,
                                  'server': neuroarch_server(user=st)}
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

    @inlineCallbacks
    def onJoin(self, details):
        self._max_concurrency = 10
        self._current_concurrency = 0
        self._invocations_served = 0
        self.user_list = user_list()

        arg_kws = ['color']

        reactor.suggestThreadPoolSize(self._max_concurrency*2)
        verb_translations = {'unhide': 'show',
                             'color': 'setcolor',
                             'keep' : 'remove',
                             'blink' : 'animate',
                             'unblink' : 'unanimate'}

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

            server = self.user_list.user(user_id)['server']
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
                print e

            try:
                if(task['format'] == 'morphology' and (not 'verb' in task or task['verb'] == 'show')):
                    yield self.call(cmd_uri,
                                    {'commands': {'reset':''}})
            except Exception as e:
                print e

            if('verb' in task and task['verb'] not in ['add','show']):
                try:
                    task['verb'] = verb_translations[task['verb']]
                except Exception as e:
                    pass

                args = []
                if 'color' in task: task['color'] = '#' + task['color']
                for kw in arg_kws:
                    if kw in task: args.append(task[kw])
                if len(args)==1: args=args[0]
                yield self.call(cmd_uri, {'commands': {task['verb']: [res, args]}})
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
        yield self.register(na_query, uri, RegisterOptions(details_arg='details',concurrency=self._max_concurrency/2))

        @inlineCallbacks
        def get_data_sub(q):
            res = q.get_as('nx').node.values()[0]
            ds = q.owned_by(cls='DataSource')
            if ds.nodes:
                res['data_source'] = [x.name for x in ds.nodes]
            else:
                ds = q.get_data_qw().owned_by(cls='DataSource')
                if ds.nodes:
                    res['data_source'] = [x.name for x in ds.nodes]
                else:
                    res['data_source'] = ['Unknown']

            subdata = q.get_data(cls=['NeurotransmitterData', 'GeneticData'],as_type='nx').node
            ignore = ['name','uname','label','class']
            key_map = {'Transmitters': 'transmitters'}#'transgenic_lines': 'Transgenic Lines'}
            for x in subdata.values():
                up_data = {(key_map[k] if k in key_map else k ):x[k] for k in x if k not in ignore}
                res.update(up_data)

            res = {'summary': res}
            if 'FlyCircuit' in res['summary']['data_source']:
                try:
                    flycircuit_data = yield self.call(six.u( 'ffbo.processor.fetch_flycircuit' ), res['summary']['name'])
                    res['summary']['flycircuit_data'] = flycircuit_data
                except:
                    pass

            arborization_data = q.get_data(cls='ArborizationData', as_type='nx').node
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

            post_syn_q = q.gen_traversal_out(['SendsTo',['InferredSynapse', 'Synapse']],['SendsTo','Neuron'],min_depth=1)
            pre_syn_q = q.gen_traversal_in(['SendsTo',['InferredSynapse', 'Synapse']],['SendsTo','Neuron'],min_depth=1)
            post_syn = post_syn_q.get_as('nx')
            pre_syn = pre_syn_q.get_as('nx')
            if post_syn.nodes() or pre_syn.nodes():
                post_rids = str(post_syn.nodes()).replace("'","")
                pre_rids = str(pre_syn.nodes()).replace("'","")


                post_map_command = "select $path from (traverse out('HasData') from %s while $depth<=1) where @class='MorphologyData'" % post_rids
                pre_map_command = "select $path from (traverse out('HasData') from %s while $depth<=1) where @class='MorphologyData'" % pre_rids

                post_map_l = [x.oRecordData['$path'] for x in q._graph.client.command(post_map_command)]
                pre_map_l = [x.oRecordData['$path'] for x in q._graph.client.command(pre_map_command)]

                post_map = {}
                pre_map = {}

                for p in post_map_l:
                    m = re.findall('\#\d+\:\d+', p)
                    if len(m)==2:
                        post_map[m[0]] = m[1]

                for p in pre_map_l:
                    m = re.findall('\#\d+\:\d+', p)
                    if len(m)==2:
                        pre_map[m[0]] = m[1]

                post_data = []
                for (syn, neu) in post_syn.edges():
                    if not (post_syn.node[syn]['class']  == 'InferredSynapse' or
                            post_syn.node[syn]['class']  == 'Synapse'):
                        continue
                    info = {'has_morph': 0, 'has_syn_morph': 0}
                    if 'N' not in post_syn.node[syn]:
                        print post_syn.node[syn]
                        info['number'] = 1
                    else:
                        info['number'] =  post_syn.node[syn]['N']
                    if neu in post_map:
                        info['has_morph'] = 1
                        info['rid'] = post_map[neu]
                    if syn in post_map:
                        info['has_syn_morph'] = 1
                        info['syn_rid'] = post_map[syn]
                        if 'uname' in post_syn.node[syn]:
                            info['syn_uname'] = post_syn.node[syn]['uname']
                    info['inferred'] = (post_syn.node[syn]['class'] == 'InferredSynapse')
                    info.update(post_syn.node[neu])
                    post_data.append(info)

                post_data = sorted(post_data, key=lambda x: x['number'])

                pre_data = []
                for (neu, syn) in pre_syn.edges():
                    if not (pre_syn.node[syn]['class']  == 'InferredSynapse' or
                            pre_syn.node[syn]['class']  == 'Synapse'):
                        continue
                    info = {'has_morph': 0, 'has_syn_morph': 0}
                    if 'N' not in pre_syn.node[syn]:
                        print pre_syn.node[syn]
                        info['number'] = 1
                    else:
                        info['number'] =  pre_syn.node[syn]['N']
                    if neu in pre_map:
                        info['has_morph'] = 1
                        info['rid'] = pre_map[neu]
                    if syn in pre_map:
                        info['has_syn_morph'] = 1
                        info['syn_rid'] = pre_map[syn]
                        if 'uname' in pre_syn.node[syn]:
                            info['syn_uname'] = pre_syn.node[syn]['uname']
                    info['inferred'] = (pre_syn.node[syn]['class'] == 'InferredSynapse')
                    info.update(pre_syn.node[neu])
                    pre_data.append(info)
                pre_data = sorted(pre_data, key=lambda x: x['number'])

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
                pre_N =  np.sum(pre_sum.values())
                pre_sum = {k: 100*float(v)/pre_N for (k,v) in pre_sum.items()}

                # Summary PostSyn Information
                post_sum = {}
                for x in post_data:
                    cls = x['name'].split('-')[0]
                    if cls in post_sum: post_sum[cls] += x['number']
                    else: post_sum[cls] = x['number']
                post_N =  np.sum(post_sum.values())
                post_sum = {k: 100*float(v)/post_N for (k,v) in post_sum.items()}

                res.update({
                    'connectivity':{
                        'post': {
                            'details': post_data,
                            'summary': {
                                'number': post_N,
                                'profile': post_sum
                            }
                        }, 'pre': {
                            'details': pre_data,
                            'summary': {
                                'number': pre_N,
                                'profile': pre_sum
                            }
                        }
                    }
                })

            returnValue({'data':res})

        def is_rid(rid):
            if isinstance(rid, basestring) and re.search('^\#\d+\:\d+$', rid):
                return True
            else:
                return False

        def get_syn_data_sub(q):
            res = q.get_as('nx').node.values()[0]
            ds = q.owned_by(cls='DataSource')
            if ds.nodes:
                res['data_source'] = [x.name for x in ds.nodes]
            else:
                ds = q.get_data_qw().owned_by(cls='DataSource')
                res['data_source'] = [x.name for x in ds.nodes]

            subdata = q.get_data(cls=['NeurotransmitterData', 'GeneticData', 'MorphologyData'],as_type='nx').node
            ignore = ['name','uname','label','class', 'x', 'y', 'z', 'r', 'parent', 'identifier', 'sample', 'morph_type']
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

            res = {'data':{'summary': res}}
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
            server = self.user_list.user(user_id)['server']
            try:
                if not is_rid(task['id']):
                    returnValue({})
                elem = server.graph.get_element(task['id'])
                q = QueryWrapper.from_objs(server.graph,[elem])
                callback = get_data_sub if elem.element_type == 'Neuron' else get_syn_data_sub
                if not (elem.element_type == 'Neuron' or elem.element_type == 'Synapse' or elem.element_type=='InferredSynapse'):
                    qn = q.gen_traversal_in(['HasData','Neuron'],min_depth=1)
                    if not qn:
                        q = q.gen_traversal_in(['HasData',['Synapse', 'InferredSynapse']],min_depth=1)
                    else:
                        q = qn
                        callback = get_data_sub
                #res = yield threads.deferToThread(get_data_sub, q)
                res = yield callback(q)
            except Exception as e:
                print e
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

            server = self.user_list.user(user_id)['server']
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

            server = self.user_list.user(user_id)['server']
            tagged_result = QueryWrapper.from_tag(graph=server.graph, tag=task['tag'])
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
            print "retrieve neuron result: " + str(res)
            return res

        uri = six.u( 'ffbo.na.retrieve_neuron.%s' % str(details.session) )
        yield self.register(retrieve_neuron, uri,RegisterOptions(concurrency=self._max_concurrency))
        print "registered %s" % uri


        # Listen for ffbo.processor.connected
        @inlineCallbacks
        def register_component():
            self.log.info( "Registering a component")
            # CALL server registration
            try:
                # registered the procedure we would like to call
                res = yield self.call(six.u( 'ffbo.server.register' ),details.session,'na','na_server_with_vfb_links')
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



if __name__ == '__main__':
    from twisted.internet._sslverify import OpenSSLCertificateAuthorities
    from twisted.internet.ssl import CertificateOptions
    import OpenSSL.crypto



    # parse command line parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output.')
    parser.add_argument('--url', dest='url', type=six.text_type, default=url,
                        help='The router URL (defaults to value from config.py)')
    parser.add_argument('--realm', dest='realm', type=six.text_type, default=realm,
                        help='The realm to join (defaults to value from config.py).')
    parser.add_argument('--ca_cert', dest='ca_cert_file', type=six.text_type,
                        default=ca_cert_file,
                        help='Root CA PEM certificate file (defaults to value from config.py).')
    parser.add_argument('--int_cert', dest='intermediate_cert_file', type=six.text_type,
                        default=intermediate_cert_file,
                        help='Intermediate PEM certificate file (defaults to value from config.py).')
    parser.add_argument('--no-ssl', dest='ssl', action='store_false')
    parser.set_defaults(ssl=ssl)
    parser.set_defaults(debug=debug)

    args = parser.parse_args()


    # start logging
    if args.debug:
        txaio.start_logging(level='debug')
    else:
        txaio.start_logging(level='info')

   # any extra info we want to forward to our ClientSession (in self.config.extra)
    extra = {'auth': True}

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
