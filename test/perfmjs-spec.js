/**
 * npm install jasmine-node -g
 * 在项目根路径下运行测试的方法： jasmine-node test/
 * 参考：https://www.npmjs.org/package/jasmine-node
 * Created by tony on 2014/7/24
 */
describe("测试perfmjs-redis-cluster", function () {
    beforeEach(function() {
        require("perfmjs-node");
    });
    it("应能测试通过redis#set方法", function() {
        require('../lib');
        perfmjs.ready(function($$, app) {
            app.register('redisCluster', $$.redisCluster);
            app.start('redisCluster');
            var redisHost = "192.168.66.47";
            var startNodes = [{host:redisHost, port:7000}, {host:redisHost, port:7001}, {host:redisHost, port:7002}];
            var redisCluster = $$.redisCluster.instance.initStartupOptions(startNodes);
            redisCluster.set('foo', 'test2', function(err, reply, redisClient) {
                if (err) {
                    $$.logger.error('error: ' + err.message);
                    return;
                }
                redisClient.get('foo', function (err, reply) {
                    if (err) {
                        $$.logger.info('redis error: ' + err);
                        return;
                    }
                    expect(1).toEqual(1);
                    $$.logger.info('reply:' + reply);
                    $$.logger.info("redisClient connected123:" + redisClient.connected);
                    redisClient.quit();
                    redisClient.end();
                    setTimeout(function() {
                        redisCluster.set('foo', 'test');
                        redisClient.send_command('asking',[], function(err, reply) {
                            if (err) {
                                $$.logger.error("error comming " + err.message);
                                return;
                            }
                            $$.logger.info('reply11:' + reply);
                        });
                        $$.logger.info("redisClient connected:" + redisClient.connected);
                    }, 5000);
                });
            });
        });
    });
});