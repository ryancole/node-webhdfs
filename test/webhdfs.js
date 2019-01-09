const username = process.env.HDFS_USERNAME || 'ryan';
const endpoint1 = process.env.HDFS_NAMENODE_1 || 'namenode1.lan';
const endpoint2 = process.env.HDFS_NAMENODE_2 || 'namenode2.lan';
const homeDir = `/user/${username}`;
const basePath = process.env.HDFS_BASE_PATH || homeDir;
const nodeOneBase = `http://${endpoint1}:50070`;
const nodeTwoBase = `http://${endpoint2}:50070`;
// endpoint defaults are written differently to verify switching

// This is only needed for testing the response parser
const webhdfs = require('..');

const should = require('should');
const nock = require('nock');


describe('WebHDFSClient', function () {

    const oneNodeClient = new (require('..')).WebHDFSClient({
        namenode_host: endpoint1
    });

    const twoNodeClient = new (require('..')).WebHDFSClient({
        user: username,
        namenode_host: endpoint1,
        namenode_list: [endpoint1, endpoint2]
    });

    // Set up a noop for testing puporses
    twoNodeClient._noop = function (callback) {
        if (callback) return callback();
        // Adding this to test the namenode switch inside the response parser
        return twoNodeClient._checkParsedResponse();
    };

    it('should set high_availability to false if a list is not provided', function (done) {
        oneNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
        oneNodeClient.options.should.have.property('high_availability', false);

        return done()
    });

    describe('high-availability capability', function () {
        it('should detect the first failover', function (done) {
            function checkBackOffSettings() {
                twoNodeClient.should.have.property('_switchedNameNodeClient', true);
                twoNodeClient.should.have.property('base_url', nodeTwoBase + '/webhdfs/v1');
                return done()
            }
            twoNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
            twoNodeClient.should.have.property('_switchedNameNodeClient', false);
            twoNodeClient.options.should.have.property('high_availability', true);
            twoNodeClient.options.should.have.property('default_backoff_period_ms', 500);
            twoNodeClient._changeNameNodeHost(checkBackOffSettings);
        });
        it('should increase backoff exponentially for repeated failovers', function (done) {
            function checkBackOffSettings() {
                twoNodeClient.should.have.property('_switchedNameNodeClient', true);
                twoNodeClient.should.have.property('_backoff_period_ms', 1000);
                twoNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
                return done()
            }
            twoNodeClient._switchedNameNodeClient = true;
            // twoNodeClient.base_url = nodeOneBase + ':50070/webhdfs/v1';
            twoNodeClient.should.have.property('base_url', nodeTwoBase + '/webhdfs/v1');
            twoNodeClient.should.have.property('_switchedNameNodeClient', true);
            twoNodeClient.options.should.have.property('high_availability', true);
            // return done()
            twoNodeClient._changeNameNodeHost(checkBackOffSettings);
        });
    });
    describe('_parseResponse', function () {
        it('executes a provided callback and resets backoff settings when there are no errors', function (done) {
            function checkParsedResponse (nullVal, bodyVal) {
                should(nullVal).be.exactly(null);
                should(bodyVal).be.exactly('ayo');
                twoNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
                twoNodeClient.should.have.property('_switchedNameNodeClient', false);
                twoNodeClient.should.have.property('_backoff_period_ms', 500);
                return done();
            }
            twoNodeClient.should.have.property('_switchedNameNodeClient', true);
            twoNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
            twoNodeClient.should.have.property('_backoff_period_ms', 1000);
            const parseResponse = webhdfs._parseResponse(twoNodeClient, '_noop', undefined, 'val1', checkParsedResponse, false)
            parseResponse(undefined, undefined, {val1: 'ayo'});
        });
        it('just checks for errors', function (done) {
            twoNodeClient.should.have.property('_switchedNameNodeClient', false);
            twoNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
            const parseResponse = webhdfs._parseResponse(twoNodeClient, undefined, undefined, undefined, undefined, true)
            should(parseResponse(undefined, undefined, undefined)).be.exactly(false);
            return done()
        });
        it('handles an hdfs StandbyException properly', function (done) {
            twoNodeClient._checkParsedResponse = function () {
                twoNodeClient.should.have.property('_switchedNameNodeClient', true);
                twoNodeClient.should.have.property('_backoff_period_ms', 500);
                twoNodeClient.should.have.property('base_url', nodeTwoBase + '/webhdfs/v1');
                return done();
            }
            twoNodeClient.should.have.property('_switchedNameNodeClient', false);
            twoNodeClient.should.have.property('_backoff_period_ms', 500);
            const parseResponse = webhdfs._parseResponse(twoNodeClient, '_noop', undefined, undefined, undefined, false)
            const result = parseResponse(undefined, undefined, {RemoteException: {exception: 'StandbyException'}});
            should(result).be.exactly(true);
        })
        it('handles a generic error properly', function (done) {
            function checkParsedResponse(error) {
                should(error).be.exactly(42);
                return done()
            }
            twoNodeClient.should.have.property('_switchedNameNodeClient', true);
            twoNodeClient.should.have.property('base_url', nodeTwoBase + '/webhdfs/v1');
            twoNodeClient.should.have.property('_backoff_period_ms', 500);
            const parseResponse = webhdfs._parseResponse(twoNodeClient, undefined, undefined, undefined, checkParsedResponse, false)
            parseResponse(42, undefined, undefined);
        })
        it('handles a refused connection properly (symptom of dead active namenode process)', function (done) {
            twoNodeClient._checkParsedResponse = function () {
                twoNodeClient.should.have.property('_switchedNameNodeClient', true);
                twoNodeClient.should.have.property('_backoff_period_ms', 1000);
                twoNodeClient.should.have.property('base_url', nodeOneBase + '/webhdfs/v1');
                return done();
            }
            twoNodeClient.should.have.property('_switchedNameNodeClient', true);
            twoNodeClient.should.have.property('_backoff_period_ms', 500);
            const parseResponse = webhdfs._parseResponse(twoNodeClient, '_noop', undefined, undefined, undefined, false)
            const result = parseResponse({code: 'ECONNREFUSED'}, undefined, undefined);
            should(result).be.exactly(true);
        })
        it('handles a not found connection error properly (symptom of dead active namenode server)', function (done) {
            twoNodeClient._checkParsedResponse = function () {
                twoNodeClient.should.have.property('_switchedNameNodeClient', true);
                twoNodeClient.should.have.property('_backoff_period_ms', 2000);
                twoNodeClient.should.have.property('base_url', nodeTwoBase + '/webhdfs/v1');
                return done();
            }
            twoNodeClient.should.have.property('_switchedNameNodeClient', true);
            twoNodeClient.should.have.property('_backoff_period_ms', 1000);
            const parseResponse = webhdfs._parseResponse(twoNodeClient, '_noop', undefined, undefined, undefined, false)
            const result = parseResponse({code: 'ENOTFOUND'}, undefined, undefined);
            should(result).be.exactly(true);
        })
        it('handles a not found connection error properly (when HA is not configured)', function (done) {
            function checkParsedResponse (error) {
                should(error.code).be.exactly('ENOTFOUND');
                return done();
            }
            const parseResponse = webhdfs._parseResponse(oneNodeClient, '_noop', undefined, undefined, checkParsedResponse, false)
            const result = parseResponse({code: 'ENOTFOUND'}, undefined, undefined);
            should(result).be.exactly(true);
        })
        it('handles a generic "RemmoteException" properly', function (done) {
            function checkParsedResponse (error) {
                should(typeof(error)).be.exactly('object')
                return done();
            }
            const parseResponse = webhdfs._parseResponse(oneNodeClient, undefined, undefined, undefined, checkParsedResponse, false)
            const result = parseResponse(undefined, undefined, {RemoteException: 'Error!'});
            should(result).be.exactly(true);
        })
    });
    describe('hdfs "delete"', function () {
        it('works properly', function (done) {
            const filePath = '/test/file';
            const qs = '?op=delete&user.name=webuser';
            const namenodeRequest= nock(nodeOneBase)
                .delete(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, {boolean: true})
            oneNodeClient.del(filePath, function (err, response) {
                should.not.exist(err);
                should(response).be.exactly(true)
                return done()
            })
        })
    })
    describe('hdfs "listStatus"', function () {
        it('works properly', function (done) {
            const fileStatus = '{"FileStatuses": {"FileStatus": [{"some": "file details"},{"some": "other file details"}]}}'
            const filePath = '/test/file';
            const qs = '?op=liststatus';
            const namenodeRequest= nock(nodeOneBase)
                .get(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, fileStatus)
            oneNodeClient.listStatus(filePath, function (err, response) {
                should.not.exist(err);
                should(response.length).be.exactly(2)
                return done()
            })
        })
    })
    describe('hdfs "getFileStatus"', function () {
        it('works properly', function (done) {
            const fileStatus = '{"FileStatus": {"accessTime": 42}}'
            const filePath = '/test/file';
            const qs = '?op=getfilestatus';
            const namenodeRequest = nock(nodeOneBase)
                .get(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, fileStatus)
            oneNodeClient.getFileStatus(filePath, function (err, response) {
                should.not.exist(err);
                should(response.accessTime).be.exactly(42)
                return done()
            })
        })
    })
    describe('hdfs "getContentSummary"', function () {
        it('works properly', function (done) {
            const contentSummary = '{"ContentSummary": {"directoryCount": 7}}'
            const filePath = '/test/file';
            const qs = '?op=getcontentsummary';
            const namenodeRequest = nock(nodeOneBase)
                .get(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, contentSummary)
            oneNodeClient.getContentSummary(filePath, function (err, response) {
                should.not.exist(err);
                should(response.directoryCount).be.exactly(7)
                return done()
            })
        })
    })
    describe('hdfs "getFileChecksum"', function () {
        it('works properly', function (done) {
            const fileChecksum = '{"FileChecksum": {"length": 42}}'
            const filePath = '/test/file';
            const qs = '?op=getfilechecksum';
            const namenodeRequest = nock(nodeOneBase)
                .get(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, fileChecksum)
            oneNodeClient.getFileChecksum(filePath, function (err, response) {
                should.not.exist(err);
                should(response.length).be.exactly(42)
                return done()
            })
        })
    })
    describe('hdfs "getHomeDirectory"', function () {
        it('works properly', function (done) {
            const homeDirectory = '{"Path": "/user/username"}'
            const urlPath = '/webhdfs/v1?op=gethomedirectory&user.name=webuser';
            const namenodeRequest = nock(nodeOneBase)
                .get(urlPath)
                .reply(200, homeDirectory)
            oneNodeClient.getHomeDirectory(function (err, response) {
                should.not.exist(err);
                should(response).be.exactly('/user/username')
                return done()
            })
        })
    })
    describe('hdfs "open"', function () {
        it('works properly', function (done) {
            const filePath = '/test/file'
            const qs = '?op=open';
            const namenodeRequest = nock(nodeOneBase)
                .get(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, 'file contents!!')
            oneNodeClient.open(filePath, function (err, status) {
                should.not.exist(err);
                should(status).be.exactly('file contents!!')
                return done()
            })
        })
    })
    describe('hdfs "rename"', function () {
        it('works properly', function (done) {
            const filePath = '/test/file';
            const newFilePath = '/new/test/file';
            const namenodeRequest = nock(nodeOneBase)
                .put('/webhdfs/v1/test/file?op=rename&destination=%2Fnew%2Ftest%2Ffile&user.name=webuser')
                .reply(200, {boolean: true})
            oneNodeClient.rename(filePath, newFilePath, function (err, status) {
                should.not.exist(err);
                should(status).be.exactly(true)
                return done()
            })
        })
    })
    describe('hdfs "mkdirs"', function () {
        it('works properly', function (done) {
            const newDir = '/new/directory'
            const qs = '?op=mkdirs&user.name=webuser';
            const namenodeRequest = nock(nodeOneBase)
                .put(`/webhdfs/v1${newDir}${qs}`)
                .reply(200, {boolean: true})
            oneNodeClient.mkdirs(newDir, function (err, response) {
                should.not.exist(err);
                should(response).be.exactly(true)
                return done()
            })
        })
    })
    describe('hdfs "append"', function () {
        const datanode = 'http://datanode1.lan:50070'
        const filePath = '/test/file'
        const appendData = 'some append data'
        const qs = '?op=append&user.name=webuser';
        it('follows redirect properly', function (done) {
            const namenodeRequest = nock(nodeOneBase)
                .post(`/webhdfs/v1${filePath}${qs}`)
                .reply(307, '', {location: `${datanode}/webhdfs/v1${filePath}${qs}`})
            const datanodeRequest = nock(datanode)
                .post(`/webhdfs/v1${filePath}${qs}`)
                .reply(200, true)
            oneNodeClient.append(filePath, appendData, function (err, response) {
                should.not.exist(err);
                should(response).be.exactly(true)
                return done()
            })
        })
    })
    describe('hdfs "create"', function () {
        const datanode = 'http://datanode1.lan:50070'
        const filePath = '/test/file'
        const appendData = 'some append data'
        const qs = '?op=create&user.name=webuser';
        it('follows redirect properly', function (done) {
            const namenodeRequest = nock(nodeOneBase)
                .put(`/webhdfs/v1${filePath}${qs}`)
                .reply(307, '', {location: `${datanode}/webhdfs/v1${filePath}${qs}`})
            const datanodeRequest = nock(datanode)
                .put(`/webhdfs/v1${filePath}${qs}`)
                .reply(201, '')
            oneNodeClient.create(filePath, appendData, function (err, response) {
                should.not.exist(err);
                should(response).be.exactly(undefined)
                return done()
            })
        })
    })
});
