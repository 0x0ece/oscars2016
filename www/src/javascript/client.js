import polyfill from 'babel/polyfill'; // eslint-disable-line no-unused-vars

import _ from 'lodash';
import React from 'react';
import ReactDOM from 'react-dom';
import Router from 'react-router';
import ga from 'react-ga';
import app from './app';
import navigateAction from './actions/navigate';
import {provideContext} from 'fluxible-addons-react';
import history from './history';

import debug from 'debug';
let appDebug = debug('App');
debug.enable('App, Fluxible, Fluxible:*');

function renderApp(context, app) {
    appDebug('React Rendering');

    ga.initialize('UA-74401232-1');

    function navigate() {
        if (window && window.location.hostname !== 'localhost') {
            ga.pageview(this.state.location.pathname);
        }
        var route = _.cloneDeep(this.state.location);
        context.executeAction(navigateAction, route);
    }

    let RouterWithContext = provideContext(Router, app.customContexts);

    ReactDOM.render(
        <RouterWithContext
                context={context.getComponentContext()}
                history={history}
                routes={app.getComponent()}
                onUpdate={navigate}/>,
        document.getElementById('app'), () => {
            appDebug('React Rendered');
        }
    );
}

appDebug('Rehydrating...');

app.rehydrate(window.App, function (err, context) {
    if (err) {
        throw err;
    }

    renderApp(context, app);
});
