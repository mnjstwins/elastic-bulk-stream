/* global require */

/*!
 * module deps
 */

const stream = require('stream')
const util = require('util')

/*!
 * globals
 */

const Transform = stream.Transform
const noop = function() {}

/**
 * ElasticBulkStream constructor
 * @param {Object} opts
 */

function ElasticBulkStream(client, opts) {
  Transform.call(this, { objectMode: true })
  this.client = client
  this.bodyMaxSize = opts.bodyMaxSize || 10
  this.flushInterval = opts.flushInterval || 1000
  this.index = opts.index
  this.body = []
}

/*!
 * extend transform
 */

util.inherits(ElasticBulkStream, Transform)

/**
 * bulk indexer stream _transform implementation
 * add doc to the internal body buffer and flush every `bodyMaxSize`
 *
 * @see http://nodejs.org/api/stream.html#stream_transform_transform_chunk_encoding_callback
 */

ElasticBulkStream.prototype._transform = function(chunk, encoding, callback) {
  this.body.push.apply(this.body, chunk)
  this.push(chunk)

  if (this.body.length >= this.bodyMaxSize) {
    this._flush(callback)
  } else if (!this.timeout) {
    this.timeout = setTimeout(this._flush.bind(this, noop), this.flushInterval)
    callback()
  } else {
    callback()
  }
}

/**
 * flush internal bugger
 * @see http://nodejs.org/api/stream.html#stream_transform_flush_callback
 */

ElasticBulkStream.prototype._flush = function(callback) {

  // reset timeout
  clearTimeout(this.timeout)
  this.timeout = null

  // do nothing if body is empty
  if (!this.body.length) return callback()

  // process bulk command
  this.client.bulk({
    body: this.body.slice(),
    index: this.index
  }, function(err) {
    callback(err)
  })

  // reset body
  this.body = []
}

/*!
 * exports
 */

module.exports.ElasticBulkStream = ElasticBulkStream
module.exports = function elasticBulkStream(client, opts) {
  return new ElasticBulkStream(client, opts)
}
