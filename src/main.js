import { Pool } from 'pg'
// import Cursor from 'pg-cursor'
import * as config from '../config'

const pool_schools = new Pool(config.db_schools)
const pool_countries = new Pool(config.db_countries)

// the pool with emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool_schools.on('error', (err, client) => {
  console.error('Unexpected error on idle client', err)
  process.exit(-1)
})

// the pool with emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool_countries.on('error', (err, client) => {
  console.error('Unexpected error on idle client', err)
  process.exit(-1)
})

function get_schools_to_geo_validate() {
  return new Promise((resolve, reject) => {
    // callback - checkout a client
    pool_schools.connect((err, client, done) => {
      if (err) throw err
      client.query('SELECT id, lat, lon, country_code from schools where date_geo_validated is null', [], (err, res) => {
        done()
        if (err) {
          console.log(err.stack)
        } else {
          resolve(res.rows);
        }
      })
    })
  })
}

function geo_validate_coordinates(obj) {
  return new Promise((resolve, reject) => {

    resolve();
    pool_countries.connect((err, client, done) => {
      if (err) throw err
      client.query("select * from all_countries_one_table WHERE ST_Within (ST_Transform (ST_GeomFromText ('POINT($1 $2)',4326),4326), all_countries_one_table.geom);", [67.587891, 67.587891], (err, res) => {
        done()
        if (err) {
          console.log(err.stack)
        } else {
          resolve(res.rows);
        }
      })
    })
  })
}

get_schools_to_geo_validate()
.then(rows => {
  rows.forEach(row => {
    return geo_validate_coordinates(row)
    .then(console.log);
  })
})
