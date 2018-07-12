const username = process.env.HDFS_USERNAME || 'ryan';
const endpoint1 = process.env.HDFS_NAMENODE_1 || 'localhost';
const endpoint2 = process.env.HDFS_NAMENODE_2 || '127.0.0.1';
const basePath = process.env.HDFS_BASE_PATH || `/user/${username}`;
// endpoint defaults are written differently to verify switching

var should = require('should');


describe('WebHDFSClient', function () {

    // never used for actual connection
    var oneNodeClient = new (require('..')).WebHDFSClient({
        namenode_host: endpoint1
    });

    var twoNodeClient = new (require('..')).WebHDFSClient({
        user: username,
        namenode_host: endpoint1,
        namenode_list: [endpoint1, endpoint2]
    });

    describe('change endpoint', function () {

        it('should set high_availability to false if a list is not provided', function (done) {
            oneNodeClient.should.have.property('base_url', 'http://' + endpoint1 + ':50070/webhdfs/v1');
            oneNodeClient.options.should.have.property('high_availability', false);

            return done()
        });

        it('should change endpoint if a list is provided', function (done) {
            twoNodeClient.should.have.property('base_url', 'http://' + endpoint1 + ':50070/webhdfs/v1');
            twoNodeClient.options.should.have.property('high_availability', true);

            twoNodeClient._changeNameNodeHost();
            twoNodeClient.should.have.property('base_url', 'http://' + endpoint2 + ':50070/webhdfs/v1');

            return done()
        });

    });
    
    
    describe('#mkdirs', function () {
        
        it('should return `true` if the directory was created', function (done) {
            
            twoNodeClient.mkdirs(basePath + '/test', function (err, success) {
                
                should.not.exist(err);
                should.exist(success);
                
                success.should.be.true;
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#getFileStatus()', function () {
        
        it('should return information about the directory', function (done) {
            
            twoNodeClient.getFileStatus(basePath + '/test', function (err, status) {
                
                should.not.exist(err);
                should.exist(status);
                
                status.should.have.property('type', 'DIRECTORY');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#create()', function () {
        
        it('should return the path to the new file', function (done) {
            
            twoNodeClient.create(basePath + '/test/foo.txt', '{"foo":"bar"}', function (err, path) {
                
                should.not.exist(err);
                should.exist(path);
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#rename()', function () {
        
        it('should return `true` if the file was renamed', function (done) {
            
            twoNodeClient.rename(basePath + '/test/foo.txt', basePath + '/test/bar.txt', function (err, success) {
                
                should.not.exist(err);
                should.exist(success);
                
                success.should.be.true;
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#getContentSummary()', function () {
        
        it('should return summary of directory content', function (done) {
            
            twoNodeClient.getContentSummary(basePath + '/test', function (err, summary) {
                
                should.not.exist(err);
                should.exist(summary);
                
                summary.should.have.property('fileCount', 1);
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#getFileChecksum()', function () {
        
        it('should return a file checksum', function (done) {
            
            twoNodeClient.getFileChecksum(basePath + '/test/bar.txt', function (err, checksum) {
                
                should.not.exist(err);
                should.exist(checksum);

                checksum.should.have.property('algorithm', 'MD5-of-0MD5-of-512CRC32C');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#open()', function () {
        
        it('should return the files content', function (done) {
            
            twoNodeClient.open(basePath + '/test/bar.txt', function (err, data) {
                
                should.not.exist(err);
                should.exist(data);
                
              (data).should.have.property('foo', 'bar');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#del()', function () {
        
        it('should return `true` if the directory was deleted', function (done) {
            
            twoNodeClient.del(basePath + '/test', { recursive: true }, function (err, success) {
                
                should.not.exist(err);
                should.exist(success);
                
                success.should.be.true;
                
                return done();
                
            });
            
        });
        
    });
    
});
