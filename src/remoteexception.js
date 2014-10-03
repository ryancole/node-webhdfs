function RemoteException( message, exception ){
    if(typeof(message) == 'object' && 'RemoteException' in message){
        this.fromResponseException(message.RemoteException);
    }
    else if(typeof(message) == 'string'){
        this.exception = exception || '_UnknownRemoteException';
        this.message = message;
    }
}

RemoteException.prototype = new Error();
RemoteException.prototype.constructor = RemoteException;

RemoteException.prototype.fromResponseException = function(re){
    this.exception = re.exception;
    this.message = re.message;
}

module.exports = RemoteException;