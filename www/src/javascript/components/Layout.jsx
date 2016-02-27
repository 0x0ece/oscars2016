// import React, {PropTypes} from 'react';
// import Parse from 'parse';
// import {IndexLink, Link} from 'react-router';
// import {Navbar, NavBrand, Nav, NavItem} from 'react-bootstrap';
// import {IndexLinkContainer, LinkContainer} from 'react-router-bootstrap';
// import ApplicationStore from '../stores/ApplicationStore';
// import {connectToStores}  from 'fluxible-addons-react';

var React = require('react');
var Parse = require('parse');
var ParseReact = require('parse-react'); //Use with mutations to update database
var b = require('react-bootstrap');
import {IndexLink, Link} from 'react-router';
import {Navbar, NavBrand, Nav, NavItem} from 'react-bootstrap';
import {IndexLinkContainer, LinkContainer} from 'react-router-bootstrap';


var Layout = module.exports = React.createClass({

  render: function() {
    return (
      <div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-sm-12 main">
              {this.props.children}
            </div>
          </div>
        </div>
      </div>
    );
  }
});

            // <div className="col-sm-3 col-md-2 sidebar">
            //   <Nav ulClassName="nav-sidebar">
            //     <IndexLinkContainer to="/"><NavItem>Overview</NavItem></IndexLinkContainer>
            //     <LinkContainer to="/link1"><NavItem>Link1</NavItem></LinkContainer>
            //     <LinkContainer to="/link2"><NavItem>Link2</NavItem></LinkContainer>
            //   </Nav>
            // </div>
            // <div className="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
