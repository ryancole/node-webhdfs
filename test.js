
var should = require('should');


describe('WebHDFSClient', function () {
    
    var client = new (require('./webhdfs')).WebHDFSClient({ user: 'ryan' });
    
    describe('#mkdirs', function () {
        
        it('should return `true` if the directory was created', function (done) {
            
            client.mkdirs('/test', function (err, success) {
                
                should.not.exist(err);
                should.exist(success);
                
                success.should.be.true;
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#getFileStatus()', function () {
        
        it('should return information about the directory', function (done) {
            
            client.getFileStatus('/test', function (err, status) {
                
                should.not.exist(err);
                should.exist(status);
                
                status.should.have.property('type', 'DIRECTORY');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#create()', function () {
        
        it('should return the path to the new file', function (done) {
            
            client.create('/test/foo.txt', JSON.stringify({ bar: 'baz' }), function (err, path) {
                
                should.not.exist(err);
                should.exist(path);
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#getContentSummary()', function () {
        
        it('should return summary of directory content', function (done) {
            
            client.getContentSummary('/test', function (err, summary) {
                
                should.not.exist(err);
                should.exist(summary);
                
                summary.should.have.property('fileCount', 1);
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#getFileChecksum()', function () {
        
        it('should return a file checksum', function (done) {
            
            client.getFileChecksum('/test/foo.txt', function (err, checksum) {
                
                should.not.exist(err);
                should.exist(checksum);
                
                checksum.should.have.property('bytes', '000002000000000000000000ecd9f26f32af06e6c4158c2a70e0f12800000000');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#open()', function () {
        
        it('should return the files content', function (done) {
            
            client.open('/test/foo.txt', function (err, data) {
                
                should.not.exist(err);
                should.exist(data);
                
                JSON.parse(data).should.have.property('bar', 'baz');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#del()', function () {
        
        it('should return `true` if the directory was deleted', function (done) {
            
            client.del('/test', { recursive: true }, function (err, success) {
                
                should.not.exist(err);
                should.exist(success);
                
                success.should.be.true;
                
                return done();
                
            });
            
        });
        
    });
    
});