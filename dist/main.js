'use strict';

var _pg = require('pg');

var _config = require('../config');

var config = _interopRequireWildcard(_config);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }

// import Cursor from 'pg-cursor'
var pool_schools = new _pg.Pool(config.db_schools);
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
      client.query('SELECT id, lat, lon, country_code from schools where date_geo_validated is null and lat is not null and lon is not null', [], function (err, res) {
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

function geo_validate_coordinates(school) {
  return new Promise(function (resolve, reject) {
    pool_countries.connect(function (err, client, done) {
      if (err) throw err;
      client.query("select count(*) from all_countries_one_table WHERE ST_Within (ST_Transform (ST_GeomFromText ('POINT(" + school.lon + " " + school.lat + ")',4326),4326), all_countries_one_table.geom);", [], function (err, res) {
        done();
        if (err) {
          console.log(err.stack);
        } else {
          resolve({
            school: school,
            rows: res.rows
          });
        }
      });
    });
  });
}

function update_row(school, object, index) {
  return new Promise(function (resolve, reject) {
    var text = 'update schools set date_geo_validated = CURRENT_TIMESTAMP, coords_within_country = $1 where id = $2;';
    var is_valid = object.rows[0].count > 0 && school.country_code === object.school.country_code;
    var values = [is_valid, object.school.id];
    pool_schools.connect(function (err, client, done) {
      if (err) throw err;
      client.query(text, values, function (err, res) {
        done();
        console.log(is_valid, school.country_code, object.school.id, err);
        resolve();
      });
    });
  });
}

function validate_and_update(school, index) {
  return new Promise(function (resolve, reject) {

    geo_validate_coordinates(school).then(function (row) {
      update_row(school, row, index).then(resolve);
    });
  });
}

get_schools_to_geo_validate().then(function (schools) {
  _bluebird2.default.each(schools, function (school, index) {
    return validate_and_update(school, index);
  }, { concurrency: 1 });
}, function () {
  console.log('All done!');
  process.exit();
});