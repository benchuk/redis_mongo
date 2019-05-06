const MongoClient = require('mongodb').MongoClient
var redis = require('redis'),
  client = redis.createClient()
var geoRedis = require('georedis').initialize(client)

const url = 'mongodb://localhost:27017'
const dbName = 'spatialTest'
const shouldInsert = false
const DISTANCE = 10000
const queryLat = 34.1
const queryLon = 31.1

MongoClient.connect(url, function(err, client) {
  if (err) {
    console.log(err)
    return
  }
  console.log('Connected successfully to server')
  //app.jsconsole.log(client)

  const db = client.db(dbName)

  console.log('=========================================')
  //console.log(db)
  db.createCollection('places', function(err, result) {
    db.collection('places').createIndex({ location: '2dsphere' })
    if (err) {
      console.log('==============ERRRRROR===========================')
    }
    console.log('*************************   Collection is created!')

    var amount = 1000

    //RunMongoTests(db, amount)

    RunRedisTests(geoRedis, amount)
    //client.close()
    // close the connection to db when you are done with it
    //db.close()
  })
  console.log('=========================================')
})

let latDelta = 0.001
let lonDelta = 0.001

function RunMongoTests(db, amount) {
  if (shouldInsert) {
    var insertStart = new Date()
    console.log('mongoDbInsertSpatialItems')
    mongoDbInsertSpatialItems(db, queryLat, queryLon, amount)
    var end = new Date() - insertStart
    console.log(
      'mongoDbInsertSpatialItems: Insert Execution time: %dms for %d items',
      end,
      amount
    )
  }
  console.log('mongoDbRunSpatialTest')
  var queryStart = new Date()
  let res = mongoDbRunSpatialTest(db)
  var end2 = new Date() - queryStart
  console.log(
    'mongoDbRunSpatialTest: Query Execution time: %dms for %d items',
    end2,
    amount
  )
}

/***************  Mongo ********************************/
function mongoDbInsertSpatialItems(db, lat, lon, count) {
  for (var i = 0; i < count; i++) {
    console.log('i: ' + i)

    lat += latDelta
    lon += lonDelta
    console.log('lat: ' + lat)
    console.log('lon: ' + lon)

    db.collection('places').insert({
      name: 'item ' + i,
      location: {
        type: 'Point',
        coordinates: [lat, lon]
      }
    })
  }
}

function mongoDbInsertSpatialItems(db, lat, lon, count) {
  for (var i = 0; i < count; i++) {
    console.log('i: ' + i)

    lat += latDelta
    lon += lonDelta
    console.log('lat: ' + lat)
    console.log('lon: ' + lon)

    db.collection('places').insert({
      name: 'item ' + i,
      location: {
        type: 'Point',
        coordinates: [lat, lon]
      }
    })
  }
}

function mongoDbRunSpatialTest(db) {
  db.collection('places')
    .find({
      location: {
        $near: {
          $geometry: { type: 'Point', coordinates: [queryLat, queryLon] },
          $minDistance: 0,
          $maxDistance: DISTANCE
        }
      }
    })
    .toArray(function(err, docs) {
      console.log(err)
      console.log(docs)
    })
}

function RunRedisTests(geoRedis, amount) {
  if (shouldInsert) {
    var insertStart = new Date()
    console.log('redisInsertSpatialItems')
    redisInsertSpatialItems(geoRedis, queryLat, queryLon, amount)
    var end = new Date() - insertStart
    console.log(
      'RunRedisTests: Insert Execution time: %dms for %d items',
      end,
      amount
    )
  }
  console.log('geoRedisRunSpatialTest')
  var queryStart = new Date()
  let res = geoRedisRunSpatialTest(geoRedis)
  var end2 = new Date() - queryStart
  console.log(
    'geoRedisRunSpatialTest: Query Execution time: %dms for %d items',
    end2,
    amount
  )
}

function redisInsertSpatialItems(geo, lat, lon, count) {
  for (var i = 0; i < count; i++) {
    console.log('i: ' + i)

    lat += latDelta
    lon += lonDelta
    console.log('lat: ' + lat)
    console.log('lon: ' + lon)

    geo.addLocation('item ' + i, { latitude: lat, longitude: lon }, function(
      err,
      reply
    ) {
      if (err) console.error(err)
      else console.log('added location:', reply)
    })
  }
}

function geoRedisRunSpatialTest(geo) {
  geo.nearby({ latitude: queryLat, longitude: queryLon }, DISTANCE, function(
    err,
    locations
  ) {
    if (err) console.error(err)
    else
      console.log(
        '******************************* nearby locations:',
        locations
      )
  })
}
