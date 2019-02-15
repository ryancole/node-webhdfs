A WebHDFS module for Node.js

## Notice

I no longer use HDFS and have no plans to continue development on this module. With that said, many of the more recent patches to this module have not been from myself, and instead have been through open-source contributions. If you would like me to add you as a maintainer of this repository then just open a pull request or somehow message me and I will add you.

# WebHDFS

I am currently following and testing against the [WebHDFS REST API documentation](http://hadoop.apache.org/docs/r1.2.1/webhdfs.html) for the `1.2.1` release, by Apache. Make sure you enable WebHDFS in the hdfs site configuration file.

# Tests

I use [Mocha](https://mochajs.org/) and [should.js](https://github.com/visionmedia/should.js) for unit testing. They will be required if you want to run the unit tests. To execute the tests, simply `npm test`, but install the requirements first.

The following environment variables are used to configure the tests:

- `HDFS_USERNAME` -- your username on the HDFS cluster (default: `ryan`)
- `HDFS_NAMENODE_1` -- hostname of your primary namenode (default: `localhost`)
- `HDFS_NAMENODE_2` -- hostname of your secondary namenode (default: `localhost`)
- `HDFS_BASE_PATH` -- directory in which to conduct tests (default: `/user/$HDFS_USERNAME`)
