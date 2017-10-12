import { Pool } from 'pg'
// import Cursor from 'pg-cursor'
import * as config from '../config'
import bluebird from 'bluebird'

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
      client.query('SELECT id, lat, lon, country_code from schools where date_geo_validated is null and lat is not null and lon is not null', [], (err, res) => {
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

function geo_validate_coordinates(school) {
  return new Promise((resolve, reject) => {
    pool_countries.connect((err, client, done) => {
      if (err) throw err
      client.query("select count(*) from all_countries_one_table WHERE ST_Within (ST_Transform (ST_GeomFromText ('POINT(" + school.lon + " " + school.lat + ")',4326),4326), all_countries_one_table.geom);", [], (err, res) => {
        done()
        if (err) {
          console.log(err.stack)
        } else {
          resolve({
            school: school,
            rows: res.rows
          });
        }
      })
    })
  })
}

function update_row(school, object, index) {
  return new Promise((resolve, reject) => {
    const text = 'update schools set date_geo_validated = CURRENT_TIMESTAMP, coords_within_country = $1 where id = $2;'
    let is_valid = object.rows[0].count > 0 && school.country_code === object.school.country_code;
    const values = [
      is_valid,
      object.school.id]
      pool_schools.connect((err, client, done) => {
      if (err) throw err
      client.query(text, values, (err, res) => {
        done()
        if (index % 100 === 0) {
          console.log(index, is_valid, school.country_code, object.school.id,  err)
        }
        resolve()
      })
    });
  })
}

function validate_and_update(school, index) {
  return new Promise((resolve, reject) => {

    geo_validate_coordinates(school)
    .then(row => {
      update_row(school, row, index)
      .then(resolve)
    })
  })
}

get_schools_to_geo_validate()
.then(schools => {
  bluebird.each(schools, (school, index) => {
    return validate_and_update(school, index)
  }, {concurrency: 1})
}, () => {
  console.log('All done!')
  process.exit();
})
