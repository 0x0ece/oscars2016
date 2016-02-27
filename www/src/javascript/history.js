import createHashHistory from 'history/lib/createHashHistory';
import createMemoryHistory from 'history/lib/createMemoryHistory';

let history;

if (typeof window !== 'undefined') {
    history = createHashHistory();
} else {
    history = createMemoryHistory();
}

export default history;
