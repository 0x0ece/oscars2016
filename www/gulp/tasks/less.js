var gulp = require('gulp'),
  less = require('gulp-less'),
  minifyCSS = require('gulp-minify-css'),
  autoprefixer = require('gulp-autoprefixer'),
  sourcemaps = require('gulp-sourcemaps'),
  handleErrors = require('../util/handleErrors'),
  config = require('../config').less;

var taskDef = function() {
  return gulp.src(config.src)
    .pipe(sourcemaps.init())
    .pipe(less())
    .on('error', handleErrors)
    .pipe(autoprefixer({cascade: false, browsers: ['last 2 versions']}))
    // .pipe(minifyCSS())
    .pipe(sourcemaps.write())
    .pipe(gulp.dest(config.dest));
};

module.exports = taskDef;

gulp.task('less', taskDef);
