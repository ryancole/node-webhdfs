A WebHDFS module for Node.js

# About

I am currently following and testing against the [WebHDFS REST API documentation](http://hadoop.apache.org/docs/r1.2.1/webhdfs.html) for the `1.2.1` release, by Apache. Make sure you enable WebHDFS in the hdfs site configuration file.

# Tests

I use [Mocha](https://mochajs.org/) and [should.js](https://github.com/visionmedia/should.js) for unit testing. They will be required if you want to run the unit tests. To execute the tests, simply `npm test`, but install the requirements first.  You will also likely need to adjust the constants in the test file first (or have a username "ryan" setup for hosts "endpoint1" and "endpoint2").
