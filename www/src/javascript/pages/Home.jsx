import React from 'react';
import EntityChart from '../components/EntityChart';

export default class Home extends React.Component{

    render () {
        return (
            <div>
                <h1>
                	Oscars 2016 on Twitter<br/>
                	<small>Insights and trending topics during the #Oscars</small>
                </h1>

                <EntityChart/>
            </div>
        );
    }
}
