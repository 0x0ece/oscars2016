import BaseStore from 'fluxible/addons/BaseStore';
import axios from 'axios';

class Twitter extends BaseStore {
    constructor (dispatcher) {
        super(dispatcher);
        this.i = 0;
        this.entity_list = [];
        this.entity_data = {};
    }

    getRandomValue() {
        return Math.random();
    }

    updateEntityChart(payload) {
        if (this.entity_list) {
            axios
              .get('http://theneeds0.appspot.com/entities_data.json', {
                params: {
                    entities: this.entity_list
                }
              })
              .then(response => {
                var newEntities = response.data.entities || {};
                // console.log("[TwitterStore] new entities: " + newEntities);

                // 
                for (var i = 0; i < this.entity_list.length; i++) {
                    var entity = this.entity_list[i];
                    var newData = newEntities[entity] || [];
                    if (newData) {
                        console.log("[TwitterStore] new data for " + entity);
                        this.entity_data[entity] = newData.map(function(el) {
                            // console.log("[TwitterStore] time: "+el.time+", count: "+el.count);
                            return {
                                x: new Date(el.time),
                                y: el.count
                            }
                        });
                    }
                }

                this.emitChange();
              })
              .catch(response => {
                console.log(response);
              });
        }

        // for (var i = 0; i < this.entity_list.length; i++) {
        //     var entity = this.entity_list[i];
        //     var newData = {
        //         x: new Date(),
        //         y: this.getRandomValue()
        //     };
        //     try {
        //         this.entity_data[entity].push(newData);
        //     } catch(e) {
        //         this.entity_data[entity] = [newData];
        //     }
        //     var newData = {
        //         x: (new Date()).setSeconds(newData.x.getSeconds() + 1),
        //         y: this.getRandomValue()
        //     };
        //     try {
        //         this.entity_data[entity].push(newData);
        //     } catch(e) {
        //         this.entity_data[entity] = [newData];
        //     }            
        // }
        // this.emitChange();
    }

    _setEntityListWithOrder(newList) {
        // reorder to keep existing elements in the same position
        for (var i = 0; i < newList.length; i++) {
            var pos = this.entity_list.indexOf(newList[i]);
            if (0 <= pos && pos < newList.length) {
                var t = newList[i];
                newList[i] = newList[pos];
                newList[pos] = t;
            }
        }

        // garbage collection
        for (var i = 0; i < this.entity_list.length; i++) {
            var el = this.entity_list[i];
            if (newList.indexOf(el) === -1 && this.entity_data[el]) {
                delete this.entity_data[el];
            }
        }

        this.entity_list = newList;
        
        this.emitChange();
    }

    setEntitySearch(payload) {
        var search = payload.search || '';
        var newList = search.split(/[^a-zA-Z0-9#@]/);
        newList = newList.filter(function (el) {
            return el && (el[0] == '#' || el[0] == '@');
        });
        if (newList) {
            this._setEntityListWithOrder(newList);
        }
    }

    setEntityInsights(payload) {
        // var insights = payload.insights || '';
        // var newList = insights.split(/[^a-zA-Z0-9#@]/);
        var newList = payload.insights.filter(function (el) {
            return el && (el[0] == '#' || el[0] == '@');
        });
        if (newList) {
            this._setEntityListWithOrder(newList);
            axios
              .get('/'+payload.insightUrl+'.json', {})
              .then(response => {
                var newEntities = response.data.entities || {};
                // console.log("[TwitterStore] new entities: " + newEntities);

                // 
                for (var i = 0; i < this.entity_list.length; i++) {
                    var entity = this.entity_list[i];
                    var newData = newEntities[entity] || [];
                    if (newData) {
                        console.log("[TwitterStore] new data for " + entity);
                        this.entity_data[entity] = newData.map(function(el) {
                            // console.log("[TwitterStore] time: "+el.time+", count: "+el.count);
                            return {
                                x: new Date(el.time),
                                y: el.count
                            }
                        });
                    }
                }

                this.emitChange();
              })
              .catch(response => {
                console.log(response);
              });
        }
    }

    updateEntityTop(payload) {
        axios
          .get('http://theneeds0.appspot.com/entities_top.json', {})
          .then(response => {
            var newTop = response.data.entities || [];
            console.log("[TwitterStore] new top: " + newTop);
            newTop = newTop.slice(0,5);

            this._setEntityListWithOrder(newTop);
          })
          .catch(response => {
            console.log(response);
          });

        // this.i++;
        // var newTop = [];
        // if (this.i % 5 == 0) {
        //     newTop = ['x'];
        // }
        // if (this.i % 5 == 1) {
        //     newTop = ['a', 'b', 'c'];
        // }
        // if (this.i % 5 == 2) {
        //     newTop = ['e', 'b', 'f'];
        // }
        // if (this.i % 5 == 3) {
        //     newTop = ['d', 'e', 'f'];
        // }
        // if (this.i % 5 == 4) {
        //     newTop = ['y', 'x'];
        // }
    }

    getState () {
        return {
            entity_list: this.entity_list,
            entity_data: this.entity_data
        };
    }

    dehydrate () {
        return this.getState();
    }

    rehydrate (state) {
        this.time = new Date(state.time);
    }

}

Twitter.storeName = 'Twitter';
Twitter.handlers = {
    'UPDATE_ENTITY_CHART': 'updateEntityChart',
    'UPDATE_ENTITY_TOP': 'updateEntityTop',
    'SET_ENTITY_SEARCH': 'setEntitySearch',
    'SET_ENTITY_INSIGHTS': 'setEntityInsights'
};

export default Twitter;
