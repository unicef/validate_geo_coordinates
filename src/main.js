import { Pool } from 'pg'
// import Cursor from 'pg-cursor'
import * as config from '../config'
import bluebird from 'bluebird'
import iso_3_2 from '../public/iso3_2'
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
      //client.query("SELECT id,name, lat, lon, country_code from schools where lat is not null and lon is not null and country_code='BR'", [], (err, res) => {
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
      client.query("select iso, name_0, ID_0, ID_1, ID_2, ID_3, ID_4, ID_5 from all_countries_one_table WHERE ST_Within (ST_Transform (ST_GeomFromText ('POINT(" + school.lon + " " + school.lat + ")',4326),4326), all_countries_one_table.geom);", [], (err, res) => {
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

function remove_pesky_quote(object) {
  return Object.keys(object.rows[0]).reduce((h, key) => {
    if (object.rows[0][key]) {
      h[key] = object.rows[0][key].replace(/('|\s+)/g, '');
    }
    return h
  }, {});
}

function update_row(school, object, index) {
  return new Promise((resolve, reject) => {
    const text = 'update schools set date_geo_validated = CURRENT_TIMESTAMP, ' +
    'coords_within_country = $1, ' +
    'id_0 = $2, ' +
    'id_1 = $3, ' +
    'id_2 = $4, ' +
    'id_3 = $5, ' +
    'id_4 = $6, ' +
    'id_5 = $7 ' +
    'where id = $8;'
    let is_valid = false;
    let iso = null;
    let shape_values = {};
    if (object.rows.length > 0) {
      // Remove pesky single quote
      shape_values = remove_pesky_quote(object)
      iso = shape_values.iso
      is_valid = object.rows.length > 0 && school.country_code === iso_3_2[iso];
    }
    const values = [
      is_valid,
      shape_values.id_0,
      shape_values.id_1,
      shape_values.id_2,
      shape_values.id_3,
      shape_values.id_4,
      shape_values.id_5,
      object.school.id]
      pool_schools.connect((err, client, done) => {
      if (err) throw err
      client.query(text, values, (err, res) => {
        done()
        if (index % 100 === 0) {
          console.log(index, is_valid, school.country_code,iso, object.school.id,  err)
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
      // row is an object with school and country iso from all_countries_one_table
      update_row(school, row, index)
      .then(resolve)
    })
  })
}

get_schools_to_geo_validate()
.then(schools => {
  console.log(schools.length);
  bluebird.each(schools, (school, index) => {
    return validate_and_update(school, index)
  }, {concurrency: 1})
  .then(() => {
    console.log('Done with all');
    process.exit();
  })
})
