
var should = require('should');


describe('WebHDFSClient', function () {
    
    var client = new (require('..')).WebHDFSClient({ user: 'ryan' });
    
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
            
            client.create('/test/foo.txt', '{"foo":"bar"}', function (err, path) {
                
                should.not.exist(err);
                should.exist(path);
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#rename()', function () {
        
        it('should return `true` if the file was renamed', function (done) {
            
            client.rename('/test/foo.txt', '/test/bar.txt', function (err, success) {
                
                should.not.exist(err);
                should.exist(success);
                
                success.should.be.true;
                
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
            
            client.getFileChecksum('/test/bar.txt', function (err, checksum) {
                
                should.not.exist(err);
                should.exist(checksum);
                
                checksum.should.have.property('algorithm', 'MD5-of-0MD5-of-512CRC32');
                
                return done();
                
            });
            
        });
        
    });
    
    describe('#open()', function () {
        
        it('should return the files content', function (done) {
            
            client.open('/test/bar.txt', function (err, data) {
                
                should.not.exist(err);
                should.exist(data);
                
                JSON.parse(data).should.have.property('foo', 'bar');
                
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