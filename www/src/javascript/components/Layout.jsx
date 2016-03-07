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
import {Navbar, NavBrand, Nav, NavItem, Button, menuitem} from 'react-bootstrap';

var FloatingActionButton = React.createClass({
  render: function() {
    var className = 'btn-circle btn-lg '+this.props.className;
    return (
      <Button className={className} onClick={this.props.onClick}>{this.props.children}</Button>
    );
  }
});

var MenuItem = React.createClass({
  render: function() {
    var className = 'button-menu color-insight'+this.props.id;
    return this.props.insight === 'top' ? (
        <IndexLink className="link-menu" to={'/'} activeClassName="active">
          <FloatingActionButton className={className}>
            {this.props.label}
          </FloatingActionButton>
        </IndexLink>
    ) : (
        <Link className="link-menu" to={this.props.insight} activeClassName="active">
          <FloatingActionButton className={className}>
            {this.props.label}
          </FloatingActionButton>
        </Link>
    );
  }

});


var Layout = module.exports = React.createClass({

  _onClickTwitter: function(e) {
    var url = encodeURIComponent('http://oscarsdata.github.io/');
    var text = encodeURIComponent("View Twitter's reaction to the #Oscars. The best of the evening, insights and trending topics");
    window.open('https://twitter.com/intent/tweet?text='+text+'&url='+url);
    e.stopPropagation();
    e.preventDefault();
    return false;
  },

  _onClickFacebook: function(e) {
    var url = encodeURIComponent('http://oscarsdata.github.io/');
    window.open('https://www.facebook.com/sharer/sharer.php?u='+url);
    e.stopPropagation();
    e.preventDefault();
    return false;
  },

  render: function() {
    return (
    <div className="container">
    <div className="row">

      <div className="page col-sm-12 col-md-10 col-md-offset-1 col-lg-8 col-lg-offset-2">

        <h1>Oscars 2016 on Twitter</h1>
        <h2 className="text-muted">Insights and trending topics during the #Oscars</h2>

        <menu>
          <MenuItem id="0" insight="top" label="Top" />
          <MenuItem id="1" insight="redcarpet" label="Red Carpet" />
          <MenuItem id="2" insight="actors" label="Actors" />
          <MenuItem id="3" insight="films" label="Films" />
          <MenuItem id="4" insight="dicaprio" label="Leo Di Caprio" />
        </menu>

        <div className="row page-main">
          <div className="col-sm-10 col-xs-12">
            {this.props.children}
          </div>

          <div className="page-share col-sm-2 col-xs-12">
            <h3>Share</h3>
              <FloatingActionButton onClick={this._onClickTwitter} linkButton={true} className="button-share color-twitter">
                Twitter
              </FloatingActionButton>
              <FloatingActionButton onClick={this._onClickFacebook} linkButton={true} className="button-share color-facebook">
                Facebook
              </FloatingActionButton>
          </div>
        </div>

        <div className="page-more">
          <h3>Read more</h3>
          <p><a href="https://medium.com/@ecesena/preparing-for-the-oscars-2016-on-twitter-realtime-oscarsdata-4bae4b7736f1" target="_blank">Preparing for the Oscars 2016 on Twitter — #realtime, #oscarsdata</a></p>
          <p><a href="https://medium.com/@_megangroves/2016-oscars-data-telling-a-story-through-trending-topics-cfbc3e373198" target="_blank">2016 Oscars Data: Telling a Story through Trending Topics</a></p>
          <p><a href="https://medium.com/@_megangroves/2016-oscars-data-hashtag-observations-37583f1fcce9" target="_blank">2016 Oscars Data: Hashtag Observations</a></p>
          <p>From last year</p>
          <p><a href="http://ecesena.github.io/oscars2015" target="_blank">Oscars 2015 on Twitter</a></p>
          <p><a href="https://medium.com/@_megangroves/oscars-2015-on-twitter-identifying-trending-topics-fb0a92702352" target="_blank">Identifying Trending Topics (2015)</a></p>
          <p><a href="https://medium.com/@albluca/analyzing-the-oscars-with-google-cloud-dataflow-a71818cb5cb" target="_blank">Analyzing data with Google Cloud Dataflow (2015)</a></p>
          <p>&nbsp;</p>
          <p>&nbsp;</p>
          <p>&nbsp;</p>
        </div>

      </div>

      </div>
      </div>
    );
  }
});

// REALTIME
//          <MenuItem id="100" insight="" label="Real Time" />
