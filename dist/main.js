'use strict';

var _pg = require('pg');

var _config = require('../config');

var config = _interopRequireWildcard(_config);

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }

var pool_schools = new _pg.Pool(config.db_schools);
// import Cursor from 'pg-cursor'

var pool_countries = new _pg.Pool(config.db_countries);

// the pool with emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool_schools.on('error', function (err, client) {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

// the pool with emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool_countries.on('error', function (err, client) {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

function get_schools_to_geo_validate() {
  return new Promise(function (resolve, reject) {
    // callback - checkout a client
    pool_schools.connect(function (err, client, done) {
      if (err) throw err;
      client.query('SELECT id, lat, lon, country_code from schools where date_geo_validated is null', [], function (err, res) {
        done();
        if (err) {
          console.log(err.stack);
        } else {
          resolve(res.rows);
        }
      });
    });
  });
}

function geo_validate_coordinates(obj) {
  return new Promise(function (resolve, reject) {

    resolve();
    pool_countries.connect(function (err, client, done) {
      if (err) throw err;
      client.query("select * from all_countries_one_table WHERE ST_Within (ST_Transform (ST_GeomFromText ('POINT($1 $2)',4326),4326), all_countries_one_table.geom);", [67.587891, 67.587891], function (err, res) {
        done();
        if (err) {
          console.log(err.stack);
        } else {
          resolve(res.rows);
        }
      });
    });
  });
}

get_schools_to_geo_validate().then(function (rows) {
  rows.forEach(function (row) {
    console.log("UUUUU");
    return geo_validate_coordinates(row).then(console.log);
  });
});