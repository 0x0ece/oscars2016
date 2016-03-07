import React from 'react';

import {Button, Input, ButtonInput, Glyphicon}  from 'react-bootstrap';
import RTChart from 'react-rt-chart';
import { Line } from 'react-chartjs';

import {connectToStores}  from 'fluxible-addons-react';
import TwitterStore from '../stores/TwitterStore';
import updateEntityChart from '../actions/updateEntityChart';
import updateEntityTop from '../actions/updateEntityTop';
import setEntitySearch from '../actions/setEntitySearch';
import setEntityInsights from '../actions/setEntityInsights';

class EntityChart extends React.Component {

    constructor(props) {
        super(props);
        // REALTIME
        // this.state = {polling: true, manual: false, chart: null};
        this.state = {polling: false, manual: false, chart: null};
    }

    getRandomValue() {
        return Math.random();
    }

    chartLegendClick(e) {
        var y = 0;
        if (typeof(e.dataSeries.visible) === "undefined" || e.dataSeries.visible) {
            e.dataSeries.visible = false;
        } else {
            e.dataSeries.visible = true;
        }
        this.state.chart.render();
    }

    componentDidMount() {
        var chart = new CanvasJS.Chart("chartContainer",
        {
          animationEnabled: true,
          zoomEnabled: true,
          
          // title:{
          //   text: "Real time top 5"
          // },
          data: [
              // {
              //   type: "spline", 
              //   showInLegend: true,
              //   legendText: "Aaa",
              //   dataPoints: []
              // },
              // {
              //   type: "spline",
              //   showInLegend: true,
              //   legendText: "Bbb",
              //   dataPoints: []
              // }
          ],
            legend: {
                cursor: "pointer",
                itemclick: this.chartLegendClick.bind(this)
            }          
        });

        this.setState({chart: chart});

        var that = this;
        // REALTIME
        // setInterval(function() {
        //     if (that.state.polling) {
        //         that.updateEntityChart();
        //     }
        // }, 15000);
        // setInterval(function() {
        //     if (that.state.polling && !that.state.manual) {
        //         that.updateEntityTop();
        //     }
        // }, 60000);

        // if (that.state.polling) {
        //     this.updateEntityTop();
        //     setTimeout(function() { that.updateEntityChart(); }, 2000);
        // }
        this.setEntityInsights();
    }

    componentDidUpdate(prevProps, prevState) {
        if (this.props.insight !== prevProps.insight) {
            this.setEntityInsights();
        }

        var chart = this.state.chart;
        var chartData = chart.options.data;
        var entitiesList = this.props.entity_list;
        var entitiesData = this.props.entity_data;

        // corrects number of entites
        while (chartData.length < entitiesList.length) {
            chartData.push({
                type: "spline", 
                showInLegend: true,
                legendText: "",
                dataPoints: []
            });
        }

        while (chartData.length > entitiesList.length) {
            chartData.pop();
        }

        for (var i = 0; i < chartData.length; i++) {
            var newData = entitiesData[ entitiesList[i] ] || [];

            if (chartData[i].legendText != entitiesList[i]) {
                chartData[i].legendText = entitiesList[i];
                chartData[i].color = this.props.colors[i];
                chartData[i].dataPoints = newData;
            } else {
                // push new data
                var dataPoints = chartData[i].dataPoints;

                if (dataPoints.length == 0) {
                    chartData[i].dataPoints = newData
                } else {
                    var lastEl = dataPoints[dataPoints.length - 1] || {};
                    var lastTime = lastEl.x || 0;
                    for (var j = 0; j < newData.length; j++) {
                        if (newData[j].x > lastTime) {
                            dataPoints.push(newData[j]);
                        }
                    }
                    if (dataPoints.length < newData.length) {
                        chartData[i].dataPoints = newData;
                    }
                }
            }
        }

        chart.render();
    }

    updateEntityChart() {
        this.context.executeAction(updateEntityChart);
    }

    updateEntityTop() {
        this.context.executeAction(updateEntityTop);
    }

    setEntityInsights() {
        this.context.executeAction(setEntityInsights, {
            insightUrl: this.props.insight,
            insights: this.props.insightEntities
        });
    }

    realTimeUpdate() {
        if (!this.state.polling && !this.state.manual) {
            // on restart, refresh top
            this.updateEntityTop();
        }
        this.setState({ polling: !this.state.polling });
    }

    setManualOn() {
        this.setState({ manual: true, polling: true });
        this.context.executeAction(setEntitySearch, {search: this.refs.input.getValue()});
    }

    setManualOff() {
        this.setState({ manual: false, polling: true });
        this.context.executeAction(updateEntityTop);
    }

    render () {
        var buttonSearch = (
            <Button bsStyle={this.state.manual ? 'primary' : 'default'} onClick={this.setManualOn.bind(this)}>Search</Button>
        );

        return (
            <div>
                <div id="chartContainer" style={{height: "300px", width: "100%"}}></div>
                <form className="form-inline">

                    <div className="page-controls">
                      <Button onClick={this.realTimeUpdate.bind(this)}>
                          <Glyphicon glyph={this.state.polling ? 'pause' : 'play'} />
                      </Button>
                      <span className="spacer" />
                      <ButtonInput bsStyle={!this.state.manual ? 'primary' : 'default'} onClick={this.setManualOff.bind(this)}>Top 5</ButtonInput>
                      <span className="spacer" />
                      or 
                      <span className="spacer" />
                    </div>
                    <Input className="input-search" type="text" ref="input" placeholder="Type #hashtags, @mentions" buttonAfter={buttonSearch} />                  

                </form>
            </div>
        );
    }
}

EntityChart.contextTypes = {
    executeAction: React.PropTypes.func.isRequired
};

EntityChart = connectToStores(EntityChart, [TwitterStore], (context, props) => {
    return context.getStore(TwitterStore).getState();
});

export default EntityChart;
