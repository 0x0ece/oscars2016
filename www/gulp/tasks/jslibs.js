'use strict';

var gulp = require('gulp');
// var rename = require('gulp-rename');
var uglify = require('gulp-uglify');
var config = require('../config').jslibs;

var taskDef = function () {
  return gulp.src(config.src)
    // This will output the non-minified version
    .pipe(gulp.dest(config.dest))
    // This will minify and rename to foo.min.js
    // .pipe(uglify())
    // .pipe(rename({ extname: '.min.js' }))
    // .pipe(gulp.dest(config.dest));
};

module.exports = taskDef;

gulp.task('jslibs', taskDef);
