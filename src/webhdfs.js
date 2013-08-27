var querystring = require('querystring'),
    request = require('request'),
    _ = require('underscore');


var WebHDFSClient = exports.WebHDFSClient = function (options) {
    
    // save specified options
    this.options = _.defaults(options || {}, {
        
        user: 'webuser',
        namenode_port: 50070,
        namenode_host: 'localhost',
        path_prefix: '/webhdfs/v1'
        
    });
    
    // save formatted base api url
    this.base_url = 'http://' + this.options.namenode_host + ':' + this.options.namenode_port + this.options.path_prefix;
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#DELETE
WebHDFSClient.prototype.del = function (path, options, callback) {
    
    // format request args
    var args = {
        json: true,
        uri: this.base_url + path,
        qs: {
            op: 'delete',
            recursive: options.recursive,
            'user.name': this.options.user
        }
    };
    
    // send http request
    request.del(args, function (error, response, body) {
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.boolean);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#LISTSTATUS
WebHDFSClient.prototype.listStatus = function (path, callback) {
    
    // format request args
    var args = { json:true, uri: this.base_url + path, qs: { op: 'liststatus' } }
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // execute callback
        return callback(null, body.FileStatuses.FileStatus)
        
    })
    
}


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETFILESTATUS
WebHDFSClient.prototype.getFileStatus = function (path, callback) {
    
    // format request args
    var args = {
        json: true,
        uri: this.base_url + path,
        qs: {
            op: 'getfilestatus'
        }
    };
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.FileStatus);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETCONTENTSUMMARY
WebHDFSClient.prototype.getContentSummary = function (path, callback) {
    
    // format request args
    var args = {
        json: true,
        uri: this.base_url + path,
        qs: {
            op: 'getcontentsummary'
        }
    };
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.ContentSummary);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETFILECHECKSUM
WebHDFSClient.prototype.getFileChecksum = function (path, callback) {
    
    // format request args
    var args = {
        json: true,
        uri: this.base_url + path,
        qs: {
            op: 'getfilechecksum'
        }
    };
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.FileChecksum);
        
    });
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETHOMEDIRECTORY
WebHDFSClient.prototype.getHomeDirectory = function (callback) {
    
    // format request args
    var args = {
        json: true,
        uri: this.base_url,
        qs: {
            op: 'gethomedirectory',
            'user.name': this.options.user
        }
    };
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // execute callback
        return callback(null, body.Path);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#OPEN
WebHDFSClient.prototype.open = function (path, callback) {
    
    // format request args
    var args = {
        uri: this.base_url + path,
        qs: {
            op: 'open'
        }
    };
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // execute callback
        return callback(null, body);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#RENAME
WebHDFSClient.prototype.rename = function (path, destination, callback) {
    
    // format request args
    var args = {
        
        json: true,
        uri: this.base_url + path,
        qs: {
            op: 'rename',
            destination: destination,
            'user.name': this.options.user
        }
        
    };
    
    // send http request
    request.put(args, function (err, res, body) {
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.boolean);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#MKDIRS
WebHDFSClient.prototype.mkdirs = function (path, callback) {
    
    // generate query string
    var args = {
        json: true,
        uri: this.base_url + path,
        qs: {
            op: 'mkdirs',
            'user.name': this.options.user
        }
    };
    
    // send http request
    request.put(args, function (error, response, body) {
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.boolean);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#APPEND
WebHDFSClient.prototype.append = function (path, data, callback) {
    
    // format request args
    var args = {
        
        json: true,
        followRedirect: false,
        uri: this.base_url + path,
        qs: {
            op: 'append',
            'user.name': this.options.user
        }
        
    };
    
    // send http request
    request.post(args, function (err, response, body) {
        
        // check for expected redirect
        if (response.statusCode == 307) {
            
            // format request args
            args = {
                
                body: data,
                uri: response.headers.location
                
            };
            
            // send http request
            request.post(args, function (err, response, body) {
                
                // check for expected response
                if (response.statusCode == 200) {
                    
                    return callback(null, true);
                    
                } else {
                    
                    return callback(new Error('expected http 200: ' + response.body));
                    
                }
                
            });
            
        } else {
            
            return callback(new Error('expected redirect'));
            
        }
        
    }.bind(this));
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#CREATE
WebHDFSClient.prototype.create = function (path, data, options, callback) {
    if(callback===undefined && typeof(options) === 'function'){
      callback=options;
      options=undefined;
    }
    
    // generate query string
    var args = {
        json: true,
        followRedirect: false,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'create',
            'user.name': this.options.user
        },options || {})
    };
    
    // send http request
    request.put(args, function (error, response, body) {
                
        // check for expected redirect
        if (response.statusCode == 307) {
            
            // generate query string
            args = {
                body: data,
                uri: response.headers.location
            };
            
            // send http request
            request.put(args, function (error, response, body) {
                
                // check for expected created response
                if (response.statusCode == 201) {
                    
                    // execute callback
                    return callback(null, response.headers.location);
                    
                } else {
                    
                    return callback(new Error('expected http 201 created'));
                    
                }
                
            });
            
        } else {
            
            return callback(new Error('expected redirect'));
            
        }
        
    }.bind(this));
    
};
