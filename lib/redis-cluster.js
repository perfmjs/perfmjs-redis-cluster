/**
 * This is a Node.js version of Redis-rb-cluster.
 * Created by tony on 2014/7/22.
 */
require("perfmjs-node");
perfmjs.plugin('redisCluster', function($$) {
    $$.base("redisCluster", {
        init: function(eventProxy) {
            this.option('eventProxy', eventProxy).on($$.sysConfig.events.moduleIsReady, function() {$$.logger.info("RedisCluster is Ready!");});
            return this;
        },

        /**
         * 初始化参数
         * @param startupNodes
         * @param options
         */
        initStartupOptions: function(startupNodes, options) {
            this.option('startupNodes', startupNodes);
            options = options || {};
            if (!options['connTimeout']) {
                options['connTimeout'] = this.option('redisClusterDefaultTimeout');
            }
            this.option('connOption', options);
            this._addCommandsFunc();
            return this;
        },

        /**
         * 获取redis client
         * @param key e.g. key is 'foo' when command: set('foo', bar')
         * @param callback
         * @param usingStartupNodeIndex e.g. 0
         */
        _getRedisClient: function(key, callback, usingStartupNodeIndex) {
            var self = this, connFromPool = this._getConnectionFromPool(key);
            if (connFromPool) {
                callback(connFromPool);
                return;
            }
            usingStartupNodeIndex = usingStartupNodeIndex || 0;
            //只获取1次slots
            for (var i = usingStartupNodeIndex; i < this.option('startupNodes').length; i++) {
                var node = this.option('startupNodes')[i];
                this._getHashSlots(node['host'], node['port'], function(err) {
                    if (err) {
                        self._getRedisClient(key, callback, ++usingStartupNodeIndex);
                        return;
                    }
                    //get redis client
                    var slotNode = self._getNodeBySlot(self._keyslot(key));
                    if (!slotNode) {
                        self._getRedisClient(key, callback, ++usingStartupNodeIndex);
                        return;
                    }
                    //$$.logger.info('key:' + key + ' matched slotNode: ' + slotNode['name']);
                    callback(self._setConnectionPool(key, self._getRedisLink(slotNode['host'], slotNode['port'])));
                });
                break;
            }
        },

        /**
         * set connection pool
         * @param key
         * @param redisClient
         * @private
         */
        _setConnectionPool: function(key, redisClient) {
            if (!key) {
                return redisClient;
            }
            if ($$.utils.keys(this.option('connectionPool')).length > this.option('maxConnections')) {
                //do nothing
                return redisClient;
            }
            return this.option('connectionPool')[key] = redisClient;
        },

        /**
         * get connection from connection pool
         * @param key
         * @private
         */
        _getConnectionFromPool: function(key) {
            if (key && this.option('connectionPool')[key]) {
                if (this.option('connectionPool')[key]['connected']) {
                    return this.option('connectionPool')[key];
                } else {
                    delete this.option('connectionPool')[key];
                }
            }
        },

        /**
         * Return the hash slot from the key
         */
        _keyslot: function(key) {
            //Only hash what is inside {...} if there is such a pattern in the key.
            //Note that the specification requires the content that is between
            //the first { and the first } after the first {. If we found {} without
            //nothing in the middle, the whole key is hashed as usually.
            if (!key) {
                return 0;
            }
            var s = key.indexOf('{');
            if(s > 0){
                var e = key.indexOf('}',s+1);
                if (e && e != s+1){
                    key = key.substring(s,e);
                }
            }
            return this.option('crc16')(key)&(this.option('redisClusterHashSlots') - 1);
        },

        _getNodeBySlot: function(slot) {
            var node;
            var specialNode = $$.joquery.newInstance(this.option('slots')).where(function(slotNode, index) {
                if ($$.utils.toNumber(slotNode['startSlot']) <= $$.utils.toNumber(slot) && $$.utils.toNumber(slotNode['endSlot']) >= $$.utils.toNumber(slot)) {
                    return true;
                }
            },true).toArray();
            if(specialNode[0]) {
                node = specialNode[0]['master'];
            } else {
                $$.logger.info("slot not exist: " + slot);
            }
            return node;
        },

        _getRedisLink: function(host, port){
            //FIXME save connect to this.option('connects')
            var redisClient = this.option('redis').createClient(port, host, this.option('connOptions'));
            redisClient.on('error',function(err){
                console.error("error on client " + err);
            });
            return redisClient;
        },

        _getHashSlots: function(host, port, callback) {
            var self = this, redisLink = this._getRedisLink(host, port);
            if (!redisLink) {
                callback($$.utils.error('get Redis Client fail on host:' + host + ", port:" + port));
                return false;
            }
            redisLink.send_command('cluster', ['nodes'], function(err, reply) {
                if (err || (typeof reply == 'undefined')) {
                    callback(err);
                    return;
                }
                //console.log("All nodes:" + reply);
                var connectedSlots = $$.joquery.newInstance(self._parseNodesToSlots(reply)).select(function(item) {
                    return item['master']['connected'] || item['slave']['connected'];
                }).orderBy(function(item) {
                    return item['startSlot'];
                }).toArray();
                self.option('slots', connectedSlots);
                var connectedSlotNodes = [];
                $$.utils.forEach(connectedSlots, function(item, index) {
                    if (item['master']['connected']) {
                        connectedSlotNodes[connectedSlotNodes.length] = item['master'];
                    }
                    if (item['slave']['connected']) {
                        connectedSlotNodes[connectedSlotNodes.length] = item['slave'];
                    }
                });
                self.option('nodes', connectedSlotNodes);
                self._populateStartupNodes();
                self.option('refresh_table_asap', false);
                //回调函数
                callback();
            });
            return true;
        },

        _parseNodesToSlots: function(nodeReply) {
            var allSlots = [], address, data, l, lines, nodes, result, _i, _len, _ref, nodes = [], result = [], lines = nodeReply.split('\n');
            for (_i = 0, _len = lines.length; _i < _len; _i++) {
                var nodeInfo = {id:'', host:'', port:'', name:'', isMaster:false, refMater:'', connected:false, slot:''};
                l = lines[_i];
                data = l.split(" ");
                if (!data || data.length < 8) {
                    continue;
                }
                address = data[1].split(':');
                nodeInfo.id = data[0];
                nodeInfo.host = address[0];
                nodeInfo.port = address[1];
                nodeInfo.name = data[1];
                nodeInfo.isMaster = (data[2].indexOf('master')>=0)?true:false;
                nodeInfo.refMater = (data[3]=='-')?'':data[3];
                if (data[7] === 'connected') {
                    nodeInfo.connected = true;
                }
                if (data[8] && (data[8].indexOf('-') > 0)) {
                    nodeInfo['slot'] = data[8];
                }
                result[result.length] = nodeInfo;
            }
            var masterNodes = $$.joquery.newInstance(result).select(function(node) {
                return node['isMaster'];
            }).toArray();
            var slaveNodes = $$.joquery.newInstance(result).select(function(node) {
                return !node['isMaster'] && (node['refMater'].length > 0);
            }).toArray();
            var slotNodes = $$.joquery.newInstance(result).select(function(node) {
                return node['isMaster'] && node['slot'].indexOf('-') > 0;
            }).toArray();
            $$.utils.forEach(slotNodes, function(node, index) {
                var slots = node['slot'].split('-');
                var thisSlot = {startSlot: slots[0], endSlot: slots[1], master:{host:node['host'], port:node['port'], name:node['name'], connected:node['connected']}, slave:{host:'', port:0, name:'', connected:false}};
                var slaveNode = $$.joquery.newInstance(slaveNodes).where(function(item) {
                    return (node['id'] === item['refMater']);
                }).toArray();
                if (slaveNode.length > 0) {
                    thisSlot['slave']['host'] = slaveNode[0]['host'];
                    thisSlot['slave']['port'] = slaveNode[0]['port'];
                    thisSlot['slave']['name'] = slaveNode[0]['name'];
                    thisSlot['slave']['connected'] = slaveNode[0]['connected'];
                }
                allSlots[allSlots.length] = thisSlot;
            });
            return allSlots;
        },

        /**
         * Flush the cache, mostly useful for debugging when we want to force
         * redirection.
         * @private
         */
        _flushSlotsCache: function() {
            this.option('nodes', []);
            this.option('slots', []);
        },

        _populateStartupNodes: function() {
            var self = this;
            $$.utils.forEach(this.option('startupNodes'), function(node, index) {
                node['name'] = self._getNodeName(node.host, node.port);
            });
            $$.utils.forEach(this.option('nodes'), function(node, index) {
                self.option('startupNodes')[self.option('startupNodes').length] = node;
            });
            var uniqNodes = $$.joquery.newInstance(this.option('startupNodes')).distinct(function(node) {
                return node['name'];
            }).toArray();
            this.option('startupNodes', uniqNodes);
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
        _getKeyFromCommand: function(args){
            var command = args[0].toLowerCase();
            if(command === 'info' || command === 'multi' || command === 'exec' || command === 'slaveof' || command === 'config' || command === 'shutdown'){
                return undefined;
            } else if (command === 'eval'|| command === 'evalsha'){
                return args[3] + "";
            } else {
                return args[1] + "";
            }
        },

        _getNodeName: function(host, port) {
            return host + ":" + port;
        },

        _addCommandsFunc: function() {
            var self = this;
            $$.utils.forEach(this.option('commands'), function(command, index) {
                $$[self.name].prototype[command.toUpperCase()]  = self._addDynamicFunc(command, function() {
                    var args = Array.prototype.slice.call(arguments) || [];
                    args.unshift(command);
                    var params = args.slice(1) || [];
                    var commandKey = self._getKeyFromCommand(args);
                    var callback = args[args.length - 1];
                    if (callback && typeof callback === "function") {
                        params = params.slice(0, params.length - 1) || [];
                    } else {
                        callback = undefined;
                    }
                    self._getRedisClient(commandKey, function(redisClient) {
                        if (!redisClient) {
                            if (callback) {
                                callback(err);
                            }
                            return;
                        }
                        redisClient[command](params, function(err, reply) {
                            if (err) {
                                $$.logger.error('error on command:' + command + ', error msg:' + err.message);
                            }
                            if (callback) {
                                callback(err, reply, redisClient);
                            }
                        });
                    });
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
        startupNodes: [], //[{host:'*.*.*.*',port:7000,name:'*.*.*.*:7000'}]
        connOption: {}, //其它连接参数，如connTimeout
        maxConnections: 30,
        redisClusterHashSlots: 16384,
        redisClusterRequestTTL: 5,  //redirect times when error or moved
        redisClusterDefaultTimeout: 5,
        slots: [], //slot num(0~16383) ---> [{startSlot:0, endSlot:5460, master:{host:'*.*.*.*', ip:7000, name:'*.*.*.*:7000'}}, slave:{host:'*.*.*.*', ip:7000, name:'*.*.*.*:7003'}}]
        nodes: [], //id --->[{host,port,name}]
        connectionPool: {}, //连接池，e.g. {'key': 'redis连接实例'}
        eventProxy: {},
        end: 0
    };
    /*for Node.js begin*/
    if (typeof module !== 'undefined' && module.exports) {
        exports = module.exports = perfmjs.redisCluster;
    }
    /*for Node.js end*/
});