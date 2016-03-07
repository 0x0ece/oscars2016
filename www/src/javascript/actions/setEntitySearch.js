
export default function setEntitySearch (actionContext, payload, done) {
    actionContext.dispatch('SET_ENTITY_SEARCH', payload);
    done();
}
