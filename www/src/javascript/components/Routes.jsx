import React from 'react';
import { Router, Route, IndexRoute, Link } from 'react-router';
import Layout from './Layout';
import Home from '../pages/Home';
// import About from '../pages/About';
// import NotFound from '../pages/NotFound';

export default (
    <Router path="/" component={Layout}>
        <IndexRoute component={Home}/>
        <Route path=":insight" component={Home}/>
    </Router>
)
