/* Notes:
   - gulp/tasks/browserify.js handles js recompiling with watchify
   - gulp/tasks/browserSync.js watches and reloads compiled files
*/

var gulp = require('gulp');
var watch = require('gulp-watch');
var config = require('../config');

var lessTask = require('./less');
var imagesTask = require('./images');

gulp.task('watch', ['watchify'], function (callback) {

    watch(config.less.watch, {
        name: 'watch-less',
        read: false
    }, lessTask);

    watch(config.images.src, {
        name: 'watch-images',
        read: false
    }, imagesTask);

    callback();

});
