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
WebHDFSClient.prototype.del = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json: true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'delete',
            'user.name': this.options.user
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.del(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.boolean);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#LISTSTATUS
WebHDFSClient.prototype.listStatus = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;
    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json:true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'liststatus'
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // execute callback
        return callback(null, body.FileStatuses.FileStatus)
        
    })
    
}


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETFILESTATUS
WebHDFSClient.prototype.getFileStatus = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json: true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'getfilestatus'
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.FileStatus);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETCONTENTSUMMARY
WebHDFSClient.prototype.getContentSummary = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;
    }

    // hdfsoptions may be omitted
    if (callback===undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json: true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'getcontentsummary'
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.ContentSummary);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETFILECHECKSUM
WebHDFSClient.prototype.getFileChecksum = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json: true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'getfilechecksum'
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.FileChecksum);
        
    });
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#GETHOMEDIRECTORY
WebHDFSClient.prototype.getHomeDirectory = function (hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback===undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json: true,
        uri: this.base_url,
        qs: _.defaults({
            op: 'gethomedirectory',
            'user.name': this.options.user
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.get(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // execute callback
        return callback(null, body.Path);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#OPEN
WebHDFSClient.prototype.open = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }

    // format request args
    var args = _.defaults({
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'open'
        }, hdfsoptions || {})
    }, requestoptions || {});

    // send http request
    return request.get(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // execute callback
        return callback(null, body);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#RENAME
WebHDFSClient.prototype.rename = function (path, destination, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        json: true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'rename',
            destination: destination,
            'user.name': this.options.user
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.put(args, function (error, res, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.boolean);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#MKDIRS
WebHDFSClient.prototype.mkdirs = function (path, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // generate query string
    var args = _.defaults({
        json: true,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'mkdirs',
            'user.name': this.options.user
        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.put(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // exception handling
        if ('RemoteException' in body)
            return callback(new Error(body.RemoteException.message));
        
        // execute callback
        return callback(null, body.boolean);
        
    });
    
};


// ref: http://hadoop.apache.org/common/docs/r1.0.2/webhdfs.html#APPEND
WebHDFSClient.prototype.append = function (path, data, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // format request args
    var args = _.defaults({
        
        json: true,
        followRedirect: false,
        uri: this.base_url + path,
        qs: _.defaults({
            op: 'append',
            'user.name': this.options.user
        }, hdfsoptions || {})
        
    }, requestoptions || {});
    
    // send http request
    request.post(args, function (error, response, body) {
        
        // forward request error
        if (error) return callback(error);
        
        // check for expected redirect
        if (response.statusCode == 307) {
            
            // format request args
            args = _.defaults({
                
                body: data,
                uri: response.headers.location
                
            }, requestoptions || {});
            
            // send http request
            request.post(args, function (error, response, body) {
                
                // forward request error
                if (error) return callback(error);
        
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
WebHDFSClient.prototype.create = function (path, data, hdfsoptions, requestoptions, callback) {
    
    // requestoptions may be omitted
    if (callback === undefined && typeof(requestoptions) === 'function') {

        callback = requestoptions;
        requestoptions = undefined;

    }

    // hdfsoptions may be omitted
    if (callback === undefined && typeof(hdfsoptions) === 'function') {

        callback = hdfsoptions;
        hdfsoptions = undefined;

    }
    
    // generate query string
    var args = _.defaults({

        json: true,
        followRedirect: false,
        uri: this.base_url + path,
        qs: _.defaults({

            op: 'create',
            'user.name': this.options.user

        }, hdfsoptions || {})
    }, requestoptions || {});
    
    // send http request
    request.put(args, function (error, response, body) {
                
        // forward request error
        if (error) return callback(error);
        
        // check for expected redirect
        if (response.statusCode == 307) {
            
            // generate query string
            args = _.defaults({
                body: data,
                uri: response.headers.location
            }, requestoptions || {});
            
            // send http request
            request.put(args, function (error, response, body) {
                
                // forward request error
                if (error) return callback(error);
        
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
