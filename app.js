const MongoClient = require('mongodb').MongoClient
const url = 'mongodb://localhost:27017'
const dbName = 'spatialTest'
let lat = 34.1
let lon = 31.1
let shouldInsert = false
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
      console.log('==============ERRRRR===========================')
    }
    console.log('*************************   Collection is created!')

    var amount = 1000
    if (shouldInsert) {
      var insertStart = new Date()
      console.log('mongoDbInsertSpatialItems')
      mongoDbInsertSpatialItems(db, lat, lon, amount)
      var end = new Date() - insertStart
      console.log('Insert Execution time: %dms for %d items', end, amount)
    }
    console.log('mongoDbRunSpatialTest')
    var queryStart = new Date()
    let res = mongoDbRunSpatialTest(db)
    var end2 = new Date() - queryStart
    console.log('Query Execution time: %dms for %d items', end2, amount)

    //client.close()
    // close the connection to db when you are done with it
    //db.close()
  })
  console.log('=========================================')
})

let latDelta = 0.001
let lonDelta = 0.001
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
          $geometry: { type: 'Point', coordinates: [lat, lon] },
          $minDistance: 0,
          $maxDistance: 100000
        }
      }
    })
    .toArray(function(err, docs) {
      console.log(err)
      console.log(docs)
    })
}
