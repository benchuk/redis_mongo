const MongoClient = require('mongodb').MongoClient
var redis = require('redis'),
  client = redis.createClient()
var geoRedis = require('georedis').initialize(client)
const fs = require('fs')

const url = 'mongodb://localhost:27017'
const dbName = 'spatialTest'
const shouldInsert = false
//const shouldInsert = false
const runMongo = true
const runRedis = true
const amount = 10000
const DISTANCE = 50000
const queryLat = 31.1
const queryLon = 31.1
const latDelta = 0.0004
const lonDelta = 0.0 //0.0001

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
    if (runMongo) {
      RunMongoTests(db, amount)
    }

    if (runRedis) {
      RunRedisTests(geoRedis, amount)
    }
  })
  console.log('=========================================')
})

function RunMongoTests(db, count) {
  if (shouldInsert) {
    var insertStart = new Date()
    console.log('mongoDbInsertSpatialItems')
    mongoDbInsertSpatialItems(db, queryLat, queryLon, count)
    var end = new Date() - insertStart
    console.log(
      'mongoDbInsertSpatialItems: Insert Execution time: %dms for %d items',
      end,
      count
    )
  }
  console.log('mongoDbRunSpatialTest')
  var queryStart = new Date()
  mongoDbRunSpatialTest(db, function() {
    var end2 = new Date() - queryStart
    console.log(
      'mongoDbRunSpatialTest: Query Execution time: %dms for %d items',
      end2,
      amount
    )
  })
}

/***************  Mongo ********************************/
function mongoDbInsertSpatialItems(db, lat, lon, count) {
  console.log('inserting mongo')
  var batch = db.collection('places').initializeOrderedBulkOp()
  for (var i = 0; i < count; i++) {
    //console.log('i: ' + i)

    lat += latDelta
    lon += lonDelta
    console.log('lat: ' + lat)
    console.log('lon: ' + lon)

    var newItem = {
      name: 'item_' + i,
      location: {
        type: 'Point',
        coordinates: [lon, lat]
      }
    }
    batch.insert(newItem)

    // Execute the operations

    // db.collection('places').insert(
    //   {
    //     name: 'item ' + i,
    //     location: {
    //       type: 'Point',
    //       coordinates: [lat, lon]
    //     }
    //   },
    //   function(err, res) {
    //     if (err) throw err
    //     console.log(i + ' document inserted')
    //   }
    // )
  }
  batch.execute(function(err, result) {
    console.log(err)
    console.log(result)
  })
  console.log('done - last lat, lon -> [lat:' + lat + ' , lon:' + lon + ']')
}

function mongoDbRunSpatialTest(db, cb) {
  let distance = DISTANCE / 1000.0 / 6378.137
  console.log('distance: ' + distance)
  db.collection('places')
    //     // .find({
    //     //   location: { $nearSphere: [queryLat, queryLon], $minDistance: distance }
    //     // })
    // .find({
    //   location: {
    //     $nearSphere: {
    //       $geometry: { type: 'Point', coordinates: [queryLat, queryLon] },
    //       $minDistance: 0,
    //       $maxDistance: DISTANCE
    //     }
    //   }
    // })
    // .find({
    //   location: {
    //     $near: {
    //       $geometry: { type: 'Point', coordinates: [queryLat, queryLon] },
    //       $minDistance: 0,
    //       $maxDistance: DISTANCE
    //     }
    //   }
    // })

    // db.collection('places')
    .aggregate([
      {
        $geoNear: {
          spherical: false,
          near: { type: 'Point', coordinates: [queryLat, queryLon] },
          distanceField: 'calculated',
          maxDistance: DISTANCE,
          includeLocs: 'location',
          limit: 100000000
        }
      }
    ])
    .toArray(function(err, docs) {
      console.log(err)
      console.log(
        '******************************* mongo nearby locations: ' + docs.length
      )
      console.log(docs[docs.length - 1])
      let data = []
      for (let item of docs) {
        data.push({
          latitude: item.location.coordinates[0],
          longitude: item.location.coordinates[1]
        })
      }
      saveToFile('mongo.csv', data)
      cb()
    })
}

function RunRedisTests(geoRedis, count) {
  if (shouldInsert) {
    var insertStart = new Date()
    console.log('redisInsertSpatialItems')
    redisInsertSpatialItems(geoRedis, queryLat, queryLon, count)
    var end = new Date() - insertStart
    console.log(
      'RunRedisTests: Insert Execution time: %dms for %d items',
      end,
      count
    )
  }
  console.log('geoRedisRunSpatialTest')
  var queryStart = new Date()
  geoRedisRunSpatialTest(geoRedis, function() {
    var end2 = new Date() - queryStart
    console.log(
      'geoRedisRunSpatialTest: Query Execution time: %dms for %d items',
      end2,
      amount
    )
  })
}

function redisInsertSpatialItems(geo, lat, lon, count) {
  console.log('inserting redis')
  for (var i = 0; i < count; i++) {
    console.log('i: ' + i)
    lat += latDelta
    lon += lonDelta
    console.log('lat: ' + lat)
    console.log('lon: ' + lon)

    geo.addLocation('item_' + i, { latitude: lat, longitude: lon }, function(
      err,
      reply
    ) {
      if (err) console.error(err)
      //else console.log('added location:', reply)
    })
  }
  console.log('done - last lat, lon -> [lat:' + lat + ' , lon:' + lon + ']')
}

function geoRedisRunSpatialTest(geo, cb) {
  var options = {
    withCoordinates: true, // Will provide coordinates with locations, default false
    //withHashes: true, // Will provide a 52bit Geohash Integer, default false
    withDistances: true, // Will provide distance from query, default false
    //order: 'ASC', // or 'DESC' or true (same as 'ASC'), default false
    units: 'm', // or 'km', 'mi', 'ft', default 'm'
    //count: 100, // Number of results to return, default undefined
    accurate: true // Useful if in emulated mode and accuracy is important, default false
  }
  geo.nearby(
    { latitude: queryLat, longitude: queryLon },
    DISTANCE,
    options,
    function(err, locations) {
      if (err) console.error(err)
      else
        console.log(
          '******************************* redis nearby locations:',
          locations.length
        )
      console.log(locations[locations.length - 1])
      console.log(locations[0])
      saveToFile('redis.csv', locations)
      cb()
    }
  )
}

function saveToFile(fileName, data) {
  var stream = fs.createWriteStream(fileName)
  stream.once('open', function(fd) {
    for (let item of data) {
      stream.write(`${item.latitude},${item.longitude}\n`)
    }

    stream.end()
  })
}
