import polyfill from 'babel/polyfill'; // eslint-disable-line no-unused-vars

import _ from 'lodash';
import React from 'react';
import Parse from 'parse';
import ReactDOM from 'react-dom';
import Router from 'react-router';
import app from './app';
import navigateAction from './actions/navigate';
import {provideContext} from 'fluxible-addons-react';
import history from './history';

import debug from 'debug';
let appDebug = debug('App');
debug.enable('App, Fluxible, Fluxible:*');

function renderApp(context, app) {
    appDebug('React Rendering');

    function navigate() {
        var route = _.cloneDeep(this.state.location);
        context.executeAction(navigateAction, route);
    }

    let RouterWithContext = provideContext(Router, app.customContexts);

    Parse.initialize("NiRbqciaEo9FiR4TXXC4awFCLWg0ppofnR8ndVVx", "lYOpZZJm3ur9NScb0iYtt4AzE8T3Rycp9RsUBBO3");

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
