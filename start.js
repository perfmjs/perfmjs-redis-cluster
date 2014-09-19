var perfmjs = require('perfmjs-node');
perfmjs.ready(function ($$, app) {
    console.log('hello, perfmjs-redis-cluster!');
    var channel = '/realtimeApp/dataChange/kc';
    app.registerAndStart(require('./lib'));
    var redisCluster = $$.redisCluster.instance.initStartupOptions($$.sysConfig.config.get("redis.clusterNodes"));
    console.log("keySlots:" + redisCluster._keyslot(channel));
    $$.utils.nextTick(function() {
        redisCluster.publish(channel, '316');
    });
    redisCluster.subscribe(channel, function (err, reply, redis) {
        if (err) {
            $$.logger.error('error Occurred at redis subscribe: ' + err.message);
            return;
        }
        redis.on("message", function (channel, message) {
            console.log('channel:' + channel + "/message=" + message);
        });
    });
});