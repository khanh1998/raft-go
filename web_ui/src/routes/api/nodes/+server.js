// src/routes/api/nodes/+server.js
import { json } from '@sveltejs/kit';
import { } from '../../../type.js';

/**
 * List of urls splitted by commas
 * @type {string}
 */
let nodeUrlsStr = import.meta.env.VITE_NODE_URLS;




/**
 * List of nodes in cluster.
 * @type {import('../../../type.js').Node[]}
 */
let nodes = nodeUrlsStr.split(',')
    .map((url) => ({
        url: 'http://' + url,
        id: 0,
        state: '',
        term: 0,
        leader_id: 0,
        cluster_time: 0,
    }));

// Utility to make a request to a node
/**
 * @param {string} command
 * @param {string} nodeUrl
 * @param {number} clientId
 * @param {number} sequenceNum
 */
async function sendCommand(command, nodeUrl, clientId, sequenceNum) {
    console.log({ command, nodeUrl, clientId, sequenceNum })
    try {
        const response = await fetch(`${nodeUrl}/cli`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 'command': command, 'client_id': clientId, 'sequence_num': sequenceNum }),
        });
        console.log({ response })

        if (!response.ok) throw new Error('Failed to connect', { cause: response.statusText });

        let data = await response.json();

        return Promise.resolve(data);
    } catch (error) {
        console.error(error);
        console.error('Error: Unable to connect to node or leader.')
        return Promise.reject(error)
    }
}

/**
 * Handles a POST request on the server side.
 *
 * @param {Object} context - The context object provided by the SvelteKit framework.
 * @param {Request} context.request - The request object containing details about the incoming POST request.
 * @returns {Promise<Response>} - The server's response, wrapped in a promise.
 */
export async function POST({ request }) {
    let nodeUrl = nodes.find(n => n.state == 'leader')?.url;
    if (!nodeUrl) {
        nodeUrl = nodes[Math.floor(Math.random() * nodes.length)].url;
    }

    const { command, clientId, sequenceNum } = await request.json();
    let result = null;

    result = await sendCommand(command, nodeUrl, clientId, sequenceNum);

    return json(result);
}

// Manage nodes: allow adding/removing nodes
export async function GET() {
    let urls = nodes.map(n => n.url);
    try {

        /**
         * @typedef {Object} ResponseObject
         * @property {string} url - The URL of the request
         * @property {Error} error - The URL of the request
         * @property {import('../../../type.js').Node} data - The data returned from the server
         */

        /**
         * @type {ResponseObject[]}
         */
        // @ts-ignore
        const responses = await Promise.all(
            urls.map(async url => {
                try {
                    const res = await fetch(url + "/info");
                    if (!res.ok) {
                        // If the response status is not OK, throw an error
                        throw new Error(`Failed to fetch from ${url}`);
                    }

                    const data = await res.json();
                    return { url, data };
                } catch (error) {
                    return { url, error: error };
                }
            })
        );

        for (let res of responses) {
            let idx = nodes.findIndex(n => n.url == res.url)
            if (res.error) {
                nodes[idx].state = 'unavailable';
            } else {
                if (idx >= 0) {
                    nodes[idx] = {
                        ...res.data,
                        url: res.url,
                    }
                }
            }
        }
    } catch (error) {
        console.error('Error fetching data:', error);
    }

    return json(nodes);
}

// export async function DELETE({ request }) {
//     const { nodeUrl } = await request.json();
//     nodes = nodes.filter(node => node !== nodeUrl);
//     return json({ success: true });
// }

// export async function POST({ request }) {
//     const { nodeUrl } = await request.json();
//     if (!nodes.includes(nodeUrl)) {
//         nodes.push(nodeUrl);
//     }
//     return json({ success: true });
// }
