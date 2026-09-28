const p = require('path')
const { EventEmitter } = require('events')

const mkdirp = require('mkdirp')
const raf = require('random-access-file')
const level = require('level')
const sub = require('subleveldown')
const grpc = require('grpc')

const { rpc, loadMetadata } = require('hyperdrive-daemon-client')
const corestore = require('random-access-corestore')
const SwarmNetworker = require('corestore-swarm-networking')

const { DriveManager, createDriveHandlers } = require('./lib/drives')
const { catchErrors, serverError, requestError } = require('./lib/errors')

try {
  var hyperfuse = require('hyperdrive-fuse')
  var { FuseManager, createFuseHandlers } = require('./lib/fuse')
} catch (err) {
  console.warn('FUSE bindings are not available on this platform.')
}
const log = require('./lib/log').child({ component: 'server' })

const argv = extractArguments()

class HyperdriveDaemon extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    this.db = level(`${storage}/db`, { valueEncoding: 'json' })
    this.opts = opts

    const dbs = {
      fuse: sub(this.db, 'fuse', { valueEncoding: 'json' }),
      drives: sub(this.db, 'drives', { valueEncoding: 'json' })
    }

    const corestoreOpts = {
      storage: path => raf(`${storage}/cores/${path}`),
      sparse: true
    }
    this.corestore = corestore(corestoreOpts.storage, corestoreOpts)
    // The root corestore should be bootstrapped with an empty default feed.
    this.corestore.default()

    this.networking = new SwarmNetworker(this.corestore, opts.network)
    this.drives = new DriveManager(this.corestore, this.networking, dbs.drives, this.opts)
    this.fuse = hyperfuse ? new FuseManager(this.megastore, this.drives, dbs.fuse, this.opts) : null

    this.drives.on('error', err => this.emit('error', err))
    this.fuse.on('error', err => this.emit('error', err))

    this._isClosed = false
    this._isReady = false

    this.ready = () => {
      if (this._isReady) return Promise.resolve()
      return this._ready()
    }
  }

  _ready () {
    return Promise.all([
      this.db.open(),
      this.networking.listen(),
      this.drives.ready(),
      this.fuse ? this.fuse.ready() : Promise.resolve()
    ]).then(() => {
      this._ready = true
    })
  }

  async close () {
    if (this._isClosed) return Promise.resolve()
    if (this.networking) await this.networking.close()
    this._isClosed = true
  }

  async cleanup () {
    if (this.fuse && this.fuse.fuseConfigured) await this.fuse.unmount()
    await this.megastore.close()
    await this.db.close()
  }
}

module.exports = async function start (opts = {}) {
  const metadata = await new Promise((resolve, reject) => {
    loadMetadata((err, metadata) => {
      if (err) return reject(err)
      return resolve(metadata)
    })
  })
  const storageRoot = opts.storage || argv.storage
  await ensureStorage()

  const daemonOpts = {}
  const bootstrapOpts = opts.bootstrap || argv.bootstrap
  if (bootstrapOpts.length) {
    if (bootstrapOpts === false && bootstrapOpts[0] === 'false') {
      daemonOpts.network = { bootstrap: false }
    } else {
      daemonOpts.network = { bootstrap: bootstrapOpts }
    }
  }
  const daemon = new HyperdriveDaemon(storageRoot, daemonOpts)
  await daemon.ready()

  const server = new grpc.Server();
  if (hyperfuse) {
    server.addService(rpc.fuse.services.FuseService, {
      ...wrap(metadata, createFuseHandlers(daemon.fuse), { authenticate: true })
    })
  }
  server.addService(rpc.drive.services.DriveService, {
    ...wrap(metadata, createDriveHandlers(daemon.drives), { authenticate: true })
  })
  server.addService(rpc.main.services.HyperdriveService, {
    ...wrap(metadata, createMainHandlers(server, daemon), { authenticate: true })
  })


  const port = opts.port || argv.port
  server.bind(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure())
  server.start()
  log.info({ port: port }, 'server listening')

  process.once('SIGINT', cleanup)
  process.once('SIGTERM', cleanup)
  process.once('unhandledRejection', cleanup)
  process.once('uncaughtException', cleanup)

  return cleanup

  async function cleanup () {
    await daemon.close()
    server.forceShutdown()
  }

  function ensureStorage () {
    return new Promise((resolve, reject) => {
      mkdirp(storageRoot, err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }
};
