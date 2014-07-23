/**
 * This is a Node.js version of Redis-rb-cluster.
 * Created by tony on 2014/7/22.
 */
require("perfmjs-node");
var async = require('async');
perfmjs.plugin('redisCluster', function($$) {
    $$.base("redisCluster", {
        init: function(eventProxy) {
            this.option('eventProxy', eventProxy).on($$.sysconfig.events.moduleIsReady, function() {$$.logger.info("RedisCluster is Ready!");});
            return this;
        },

        /**
         * 初始化redis cluster连接
         * @param startupNodes, e.g. [{host:'xxx', port:7000},{host:'xxx', port:7001}]
         * @param max_connections, e.g. 10
         * @param options, e.g. {connect_timeout: 5}
         */
        createClient: function(startupNodes, options) {
            this.option('startupNodes', startupNodes);
            options = options || {};
            if (!options['connect_timeout']) {
                options['connect_timeout'] = this.option('redisClusterDefaultTimeout');
            }
            this.option('connOption', options);
            this._addCommandsFunc();
            this._initialize_slots_cache();
        },

        _initialize_slots_cache: function() {
            var self = this, successOne = false;
            $$.utils.forEach(this.option('startupNodes'), function(node, index) {
                //FIXME 应控制只成功执行一次循环
                if (successOne) return;
                successOne = self._getHashSlots(node['host'], node['port']);
            });
        },

        _getHashSlots: function(host, port) {
            var self = this, redisLink = this._get_redis_link(host, port);
            if (!redisLink) {
                return false;
            }
            redisLink.send_command('cluster', ['slots'], function(err, reply) {
                if (err || (typeof reply == 'undefined')) {
                    return;
                }
                console.log("======" + reply.join('--'));
                for (var i = 0; i < reply.length; i++) {
                    var masterInfo = reply[i][2], slaveInfo = reply[i][3];
                    var thisSlot = {startSlot: reply[i][0], endSlot: reply[i][1], master:{}, slave:{}};
                    thisSlot['master'] = self._toNode(masterInfo[0], masterInfo[1]);
                    thisSlot['slave'] = self._toNode(slaveInfo[0], slaveInfo[1]);
                    self.option('nodes')[self.option('nodes').length] = thisSlot['master'];
                    self.option('nodes')[self.option('nodes').length] = thisSlot['slave'];
                    self.option('slots')[self.option('slots').length] = thisSlot;
                }
                self._populate_startup_nodes();
                self.option('refresh_table_asap', false);
            });
            return true;
        },

        /**
         * Flush the cache, mostly useful for debugging when we want to force
         * redirection.
         * @private
         */
        _flush_slots_cache: function() {
            this.option('nodes', []);
            this.option('slots', []);
        },

        _populate_startup_nodes: function() {
            var self = this;
            $$.utils.forEach(this.option('startupNodes'), function(node, index) {
                node['name'] = self._get_node_name(node.host, node.port);
            });
            $$.utils.forEach(this.option('nodes'), function(node, index) {
                self.option('startupNodes')[self.option('startupNodes').length] = node;
            });
            var ss = this.option('startupNodes');
            var uniqNodes = $$.joquery.newInstance(this.option('startupNodes')).distinct(function(node) {
                return node['name'];
            }).toArray();
            this.option('startupNodes', uniqNodes);
        },

        /**
         * Return the hash slot from the key
         */
        _keyslot: function(key) {
            //Only hash what is inside {...} if there is such a pattern in the key.
            //Note that the specification requires the content that is between
            //the first { and the first } after the first {. If we found {} without
            //nothing in the middle, the whole key is hashed as usually.
            var s = key.indexOf('{');
            if(s > 0){
                var e = key.indexOf('}',s+1);
                if (e && e != s+1){
                    key = key.substring(s,e);
                }
            }
            return this.option('crc16')(key)&(this.option('redisClusterHashSlots') - 1);
        },

        /**
         * Return the first key in the command arguments.
         Currently we just return argv[1], that is, the first argument
         after the command name.
         This is indeed the key for most commands, and when it is not true
         the cluster redirection will point us to the right node anyway.
         For commands we want to explicitly bad as they don't make sense
         in the context of cluster, nil is returned.
         * @param args
         * @returns {*}
         * @private
         */
        _get_key_from_command: function(args){
            var command = args[0].toLowerCase();
            if(command === 'info' || command === 'multi' || command === 'exec' || command === 'slaveof' || command === 'config' || command === 'shutdown'){
                return undefined;
            } else if (command === 'eval'|| command === 'evalsha'){
                return args[3].toString();
            } else {
                return args[1].toString();
            }
        },

        /**
         * If the current number of connections is already the maximum number
         * allowed, close a random connection. This should be called every time
         * we cache a new connection in the @connections hash.
         */
        _close_existing_connection: function() {
            var connKeys = $$.utils.keys(this.option('connections'));
            if (connKeys.length > this.option('max_connections')) {
                var shouldDelNum = connKeys.length - this.option('max_connections');
                for (var i = 0; i < shouldDelNum; i++) {
                    var node = this.option('connections')[connKeys[i]];
                    node.quit();
                    node.end();
                }
            }
        },

        _get_redis_link: function(host, port){
            var name = this._get_node_name(host, port);
            if(!this.option('connections')[name] || !this.option('connections')[name].connected){
                if(this.option('connections')[name]) {
                    this.option('connections')[name].end();
                    delete this.option('connections')[name];
                }
                var redisClient = this.option('redis').createClient(port, host, this.option('connOptions'));
                this.option('connections')[name] = redisClient;
                redisClient.on('error', function(err) {
                    $$.logger.error("error on create redis client " + err);
                    redisClient.end();
                });
            }
            return this.option('connections')[name];
        },

        /**
         * Return a link to a random node, or raise an error if no node can be
         * contacted. This function is only called when we can't reach the node
         * associated with a given hash slot, or when we don't know the right
         * mapping.
         * The function will try to get a successful reply to the PING command,
         * otherwise the next node is tried.
         * @returns {*}
         * @private
         */
        _get_random_connection: function() {
            var conn, self = this, nodes = this.option('startupNodes').slice().sort(function() {return 0.5 - Math.random()}); //shuffle nodes
            async.detectSeries(nodes,function(node, callback) {
                node['name'] = self._get_node_name(node['host'], node['port']);
                conn = self.option('connections')[node.name];
                if (!conn || !conn.connected){
                    conn = self._get_redis_link(node.host, node.port);
                }
                conn.send_command("ping", [], function(err ,reply){
                    if(!err && reply.toString() === "PONG"){
                        //self.connections[node.name] = conn;
                        callback(true);
                    }else{
                        conn.end();
                        delete self.option('connections')[node.name];
                        callback(false);
                    }
                });
            },function(result){
                if(result){
                    return conn;
                }else{
                    return null;
                }
            });
            return this.option('connections')[nodes[0].name];
        },

        /**
         * Given a slot return the link (Redis instance) to the mapped node.
         * Make sure to create a connection with the node if we don't have
         * one
         * @param slot
         * @returns {*}
         * @private
         */
        _get_connection_by_slot: function(slot) {
            var node = this._get_node_by_slot(slot);
            if(!node){
                return this._get_random_connection();
            }
            node['name'] = this._get_node_name(node['host', node['port']]);
            if(!this.option('connections')[node.name] || !this.option('connections')[node.name].connected){
                //close_existing_connection();
                //this.connections[node.name] = this.get_redis_link(node.host,node.port,this.opt);
                this._get_redis_link(node.host, node.port);
            }
            return this.connections[node.name];
        },

        _get_node_by_slot: function(slot) {
            var node;
            var specialNode = $$.joquery.newInstance(this.option('slots')).where(function(slotNode, index) {
                if (slotNode['startNode'] <= slot && slotNode['endNode'] >= slot) {
                    return true;
                }
            },true).toArray();
            if(specialNode[0]) {
                node = specialNode[0]['master'];
            }else{
                console.warn("slot not exist "+slot);
            }
            return node;
        },

        /**
         * Dispatch commands.
         * @param args
         * @private
         */
        _send_cluster_command: function(args) {
            var r,
                self = this,
                command = args[0],
                params = args.slice(1) || [],
                asking = false,
                cb = args[args.length - 1],
                key = this._get_key_from_command(args),
                slot = this._keyslot(key),
                try_random_node = false;

            if(cb && typeof cb === "function"){
                params = params.slice(0,params.length-1) || [];
            }else{
                cb=undefined;
            }

            //push queue
            //console.log("cmd = "+ command +" params = "+params +" key= "+key+" slot ="+slot);
            async.retry(this.option('redisClusterRequestTTL'), function(callback, result) {
                //get connection
                if(try_random_node) {
                    try_random_node = false;
                    r = self._get_random_connection();
                } else {
                    r = self._get_connection_by_slot(slot);
                }
                if(!r){
                    return callback(true,"ERROR: no useable connection to slot " + slot);
                }
                //asking if need
                if(asking){
                    r.send_command('asking', []);
                }
                //send command
                //r.send_command(command,params,function(err,reply){
                r[command](params, function(err, reply){
                    if(err){
                        var errMsg = err.toString();
                        console.error("error on "+command+":"+errMsg);
                        if(errMsg.match("MOVED") || errMsg.match("ASK")) {
                            if(errMsg.match("ASK")){
                                asking = true;
                            }else{
                                if(!self.refresh_flag){
                                    self.refresh_table_asap = true;
                                }
                            }
                            var msg = err.toString().split(" "),
                            newslot = msg[2],
                            address = msg[3].split(":");
                            self.option('nodes')[self.option('nodes').length] = self._toNode(address[0], address[1]);
                            var uniqNodes = $$.joquery.newInstance(self.option('nodes')).distinct(function(node) {
                                return node['name'];
                            }).toArray();
                            self.option('nodes', uniqNodes);

                            //console.log(JSON.stringify(params)+" slotssss "+newslot + ": "+JSON.stringify(self.slots[newslot]));
                            err = "Error: Redirect";
                        } else if (errMsg.match("connection|CLUSTERDOWN|ECONNREFUSED|ETIMEDOUT|CannotConnectError|EACCES")){
                            try_random_node = true;
                        }
                    }
                    callback(err, reply);
                });
            },function(err,result){
                if(cb) cb(err,result);
            });
        },

        _toNode: function(host, port) {
            return {host:host, port:port, name:this._get_node_name(host, port)};
        },

        _get_node_name: function(host, port) {
            return host + ":" + port;
        },

        _addCommandsFunc: function() {
            var self = this;
            $$.utils.forEach(this.option('commands'), function(command, index) {
                $$[self.name].prototype[command.toUpperCase()]  = self._addDynamicFunc(command, function() {
                    var args = Array.prototype.slice.call(arguments) || [];
                    args.unshift(command);
                    return self._send_cluster_command(args);
                });
            });
        },

        /**
         * 动态增加函数
         * @param funcName
         * @param fn
         * @private
         */
        _addDynamicFunc: function(funcName, fn) {
            if ($$[this.name].prototype[funcName]) {
                return $$[this.name].prototype[funcName];
            }
            return $$[this.name].prototype[funcName] = fn;
        },
        end: 0
    });
    $$.redisCluster.defaults = {
        redis: require('redis'),
        crc16: require('./crc16'),
        commands: require('./commands'),
        startupNodes: [], //[{host:'xxx',port:7000}]
        connOption: {}, //其它连接参数，如connect_timeout
        max_connections: 10,
        redisClusterHashSlots: 16384,
        redisClusterRequestTTL: 5,  //redirect times when error or moved
        redisClusterDefaultTimeout: 5,
        refresh_table_asap: false,
        connections: {}, //{'host:port':redisClient}
        slots: [], //slot num(0~16383) ---> [{startSlot:0, endSlot:5460, master:{host:'*.*.*.*', ip:7000, name:'*.*.*.*:7000'}}, slave:{host:'*.*.*.*', ip:7000, name:'*.*.*.*:7003'}}]
        nodes: [], //id --->[{host,port,name}]
        eventProxy: {},
        end: 0
    };
    /*for Node.js begin*/
    if (typeof module !== 'undefined' && module.exports) {
        exports = module.exports = perfmjs.redisCluster;
    }
    /*for Node.js end*/
});