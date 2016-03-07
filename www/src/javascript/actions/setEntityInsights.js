
export default function setEntityInsights (actionContext, payload, done) {
    actionContext.dispatch('SET_ENTITY_INSIGHTS', payload);
    done();
}
