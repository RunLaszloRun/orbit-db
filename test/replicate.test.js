'use strict'

const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')
const OrbitDB = require('../src/OrbitDB')
const config = require('./utils/config')
const startIpfs = require('./utils/start-ipfs')
const waitForPeers = require('./utils/wait-for-peers')

const dbPath1 = './orbitdb/tests/replication/1'
const dbPath2 = './orbitdb/tests/replication/2'
const ipfsPath1 = './orbitdb/tests/replication/1/ipfs'
const ipfsPath2 = './orbitdb/tests/replication/2/ipfs'

describe('orbit-db - Replication', function() {
  this.timeout(config.timeout)

  let ipfs1, ipfs2, orbitdb1, orbitdb2, db1, db2

  before(async () => {
    config.daemon1.repo = ipfsPath1
    config.daemon2.repo = ipfsPath2
    rmrf.sync(config.daemon1.repo)
    rmrf.sync(config.daemon2.repo)
    rmrf.sync(dbPath1)
    rmrf.sync(dbPath2)
    ipfs1 = await startIpfs(config.daemon1)
    ipfs2 = await startIpfs(config.daemon2)
    // Connect the peers manually to speed up test times
    await ipfs2.swarm.connect(ipfs1._peerInfo.multiaddrs._multiaddrs[0].toString())
    await ipfs1.swarm.connect(ipfs2._peerInfo.multiaddrs._multiaddrs[0].toString())
    orbitdb1 = new OrbitDB(ipfs1, dbPath1)
    orbitdb2 = new OrbitDB(ipfs2, dbPath2)
  })

  after(async () => {
    if(orbitdb1) 
      await orbitdb1.stop()

    if(orbitdb2) 
      await orbitdb2.stop()

    // if (ipfs1)
    //   await ipfs1.stop()

    // if (ipfs2)
    //   await ipfs2.stop()
  })

  describe('two peers', function() {
    beforeEach(async () => {
      let options = { 
        // Set write access for both clients
        write: [
          orbitdb1.key.getPublic('hex'), 
          orbitdb2.key.getPublic('hex')
        ],
      }

      options = Object.assign({}, options, { path: dbPath1 })
      db1 = await orbitdb1.eventlog('replication-tests', options)
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { path: dbPath2, sync: true })
      db2 = await orbitdb2.eventlog(db1.address.toString(), options)

      assert.equal(db1.address.toString(), db2.address.toString())

      await waitForPeers(ipfs1, [orbitdb2.id], db1.address.toString())
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
    })

    afterEach(async () => {
      await db1.drop()
      await db2.drop()
    })

    it('replicates database of 1 entry', async () => {
      await db1.add('hello')
      return new Promise(resolve => {
        setTimeout(() => {
          const items = db2.iterator().collect()
          assert.equal(items.length, 1)
          assert.equal(items[0].payload.value, 'hello')
          resolve()
        }, 1000)
      })
    })

    it('replicates database of 100 entries', async () => {
      const entryCount = 100
      const entryArr = []
      let timer

      for (let i = 0; i < entryCount; i ++)
        entryArr.push(i)

      await mapSeries(entryArr, (i) => db1.add('hello' + i))

      return new Promise(resolve => {
        timer = setInterval(() => {
          const items = db2.iterator({ limit: -1 }).collect()
          if (items.length === entryCount) {
            clearInterval(timer)
            assert.equal(items.length, entryCount)
            assert.equal(items[0].payload.value, 'hello0')
            assert.equal(items[items.length - 1].payload.value, 'hello99')
            resolve()
          }
        }, 1000)
      })
    })

    it('emits correct replication info', (done) => {
      let finished = false
      let eventCount = { 'replicate': 0, 'replicate.progress': 0, 'replicated': 0 }
      let events = []
      let expectedEventCount = 99

      db2.events.on('replicate', (address, entry) => {
        eventCount['replicate'] ++
        events.push({ 
          event: 'replicate', 
          count: eventCount['replicate'], 
          entry: entry,
        })
      })

      db2.events.on('replicate.progress', (address, hash, entry, progress) => {
        eventCount['replicate.progress'] ++
        events.push({ 
          event: 'replicate.progress', 
          count: eventCount['replicate.progress'], 
          entry: entry ,
          replicationInfo: {
            max: db2._replicationInfo.max,
            progress: db2._replicationInfo.progress,
            have: db2._replicationInfo.have,
          },
        })
      })

      db2.events.on('replicated', (address) => {
        eventCount['replicated'] ++
        events.push({ 
          event: 'replicated', 
          count: eventCount['replicate'], 
          replicationInfo: {
            max: db2._replicationInfo.max,
            progress: db2._replicationInfo.progress,
            have: db2._replicationInfo.have,
          },
        })
        // Resolve with a little timeout to make sure we 
        // don't receive more than one event
        setTimeout(() => {
          if (eventCount['replicated'] === expectedEventCount)
            finished = true
        }, 500)
      })

      let timer = setInterval(() => {
        if (finished) {
          clearInterval(timer)

          assert.equal(eventCount['replicate'], expectedEventCount)
          assert.equal(eventCount['replicate.progress'], expectedEventCount)
          assert.equal(eventCount['replicated'], expectedEventCount)

          const replicateEvents = events.filter(e => e.event === 'replicate')
          assert.equal(replicateEvents.length, expectedEventCount)
          assert.equal(replicateEvents[0].entry.payload.value.split(' ')[0], 'hello')
          assert.equal(replicateEvents[0].entry.clock.time, 1)

          const replicateProgressEvents = events.filter(e => e.event === 'replicate.progress')
          assert.equal(replicateProgressEvents.length, expectedEventCount)
          assert.equal(replicateProgressEvents[0].entry.payload.value.split(' ')[0], 'hello')
          assert.equal(replicateProgressEvents[0].entry.clock.time, 1)
          assert.equal(replicateProgressEvents[0].replicationInfo.max, 1)
          assert.equal(replicateProgressEvents[0].replicationInfo.progress, 1)
          assert.deepEqual(replicateProgressEvents[0].replicationInfo.have, { 1: true })

          const replicatedEvents = events.filter(e => e.event === 'replicated')
          assert.equal(replicatedEvents.length, expectedEventCount)
          assert.equal(replicatedEvents[0].replicationInfo.max, 1)
          assert.equal(replicatedEvents[0].replicationInfo.progress, 1)
          assert.deepEqual(replicatedEvents[0].replicationInfo.have, { 1: true })

          done()
        }
      }, 100)

      // Trigger replication
      let adds = []
      for (let i = 0; i < expectedEventCount; i ++) {
        adds.push(i)
      }

      mapSeries(adds, i => db1.add('hello ' + i))
    })

    it('emits correct replication info on fresh replication', async () => {
      return new Promise(async (resolve, reject) => {
        let finished = false
        let eventCount = { 'replicate': 0, 'replicate.progress': 0, 'replicated': 0 }
        let events = []
        let expectedEventCount = 512

        // Close second instance
        await db2.close()
        await db2.drop()

        // Trigger replication
        let adds = []
        for (let i = 0; i < expectedEventCount; i ++) {
          adds.push(i)
        }

        const add = async (i) => {
          process.stdout.write("\rWriting " + (i + 1) + " / " + expectedEventCount)
          await db1.add('hello ' + i)
        }

        await mapSeries(adds, add)
        console.log()

        // Open second instance again
        let options = {
          path: dbPath2, 
          overwrite: true,
          sync: true,
          // Set write access for both clients
          write: [
            orbitdb1.key.getPublic('hex'), 
            orbitdb2.key.getPublic('hex')
          ],
        }
        db2 = await orbitdb2.eventlog(db1.address.toString(), options)

        let current = 0
        let total = 0

        db2.events.on('replicate', (address, entry) => {
          eventCount['replicate'] ++
          total = db2._replicationInfo.max
          // console.log("[replicate] ", '#' + eventCount['replicate'] + ':', current, '/', total)
          events.push({ 
            event: 'replicate', 
            count: eventCount['replicate'], 
            entry: entry,
          })
        })

        db2.events.on('replicate.progress', (address, hash, entry) => {
          eventCount['replicate.progress'] ++
          current = db2._replicationInfo.progress
          // console.log("[progress]  ", '#' + eventCount['replicate.progress'] + ':', current, '/', total)
          assert.equal(db2._replicationInfo.progress, eventCount['replicate.progress'])
          events.push({ 
            event: 'replicate.progress', 
            count: eventCount['replicate.progress'], 
            entry: entry ,
            replicationInfo: {
              max: db2._replicationInfo.max,
              progress: db2._replicationInfo.progress,
              have: db2._replicationInfo.have,
            },
          })
        })

        db2.events.on('replicated', (address, length) => {
          eventCount['replicated'] += length
          current = db2._replicationInfo.progress
          console.log("[replicated]", '#' + eventCount['replicated'] + ':', current, '/', total)
          events.push({ 
            event: 'replicated', 
            count: eventCount['replicate'], 
            replicationInfo: {
              max: db2._replicationInfo.max,
              progress: db2._replicationInfo.progress,
              have: db2._replicationInfo.have,
            },
          })
          // Resolve with a little timeout to make sure we 
          // don't receive more than one event
          setTimeout(() => {
            if (eventCount['replicated'] === expectedEventCount
                && eventCount['replicate.progress'] === expectedEventCount)
              finished = true
          }, 500)
        })

        const st = new Date().getTime()
        let timer = setInterval(() => {
          if (finished) {
            clearInterval(timer)

            const et = new Date().getTime()
            console.log("Duration:", et - st, "ms")

            assert.equal(eventCount['replicate'], expectedEventCount)
            assert.equal(eventCount['replicate.progress'], expectedEventCount)
            assert.equal(eventCount['replicated'], expectedEventCount)

            const replicateEvents = events.filter(e => e.event === 'replicate')
            assert.equal(replicateEvents.length, expectedEventCount)
            assert.equal(replicateEvents[0].entry.payload.value.split(' ')[0], 'hello')
            assert.equal(replicateEvents[0].entry.clock.time, expectedEventCount)

            const replicateProgressEvents = events.filter(e => e.event === 'replicate.progress')
            assert.equal(replicateProgressEvents.length, expectedEventCount)
            assert.equal(replicateProgressEvents[0].entry.payload.value.split(' ')[0], 'hello')
            assert.equal(replicateProgressEvents[0].entry.clock.time, expectedEventCount)
            assert.equal(replicateProgressEvents[0].replicationInfo.max, expectedEventCount)
            assert.equal(replicateProgressEvents[0].replicationInfo.progress, 1)
            let obj = {}
            obj[expectedEventCount.toString()] = true
            assert.deepEqual(replicateProgressEvents[0].replicationInfo.have, obj)

            const replicatedEvents = events.filter(e => e.event === 'replicated')
            assert.equal(replicatedEvents[0].replicationInfo.max, expectedEventCount)
            // assert.equal(replicatedEvents[0].replicationInfo.progress, 1)
            assert.deepEqual(replicatedEvents[0].replicationInfo.have[expectedEventCount.toString()], true)

            resolve()
          }
        }, 100)
      })
    })
  })
})
