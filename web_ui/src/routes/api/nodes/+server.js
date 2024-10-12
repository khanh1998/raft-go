// src/routes/api/nodes/+server.js
import { json } from '@sveltejs/kit';

let nodeUrlsStr = import.meta.env.VITE_NODE_URLS;
let nodes = nodeUrlsStr.split(',').map((/** @type {string} */ url) => ({ url: 'http://' + url, leader: false }));
/**
     * @type {any | null}
     */
let leaderNode = null;

// Set a node as the leader
/**
 * @param {string} url
 */
function markLeader(url) {
    for (let i = 0; i < nodes.length; i++) {
        nodes[i].leader = nodes[i].url == url;
        if (nodes[i].leader) { leaderNode = nodes[i] }
    }
}

// Utility to make a request to a node
/**
 * @param {string} command
 * @param {string} nodeUrl
 * @param {number} clientId
 * @param {number} sequenceNum
 */
async function sendCommand(command, nodeUrl, clientId, sequenceNum) {
    try {
        const response = await fetch(`${nodeUrl}/cli`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 'command': command, 'client_id': clientId, 'sequence_num': sequenceNum }),
        });

        if (!response.ok) throw new Error('Failed to connect');

        const data = await response.json();

        // If the node is not the leader, retry with the leader URL
        if (data?.response == 'NOT_LEADER') {
            let leaderUrl = 'http://' + data?.leader_hint;
            markLeader(leaderUrl)
            return sendCommand(command, leaderUrl, clientId, sequenceNum);
        } else {
            markLeader(nodeUrl)
        }

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
    let nodeUrl;

    if (leaderNode) {
        nodeUrl = leaderNode.url;
    } else {
        nodeUrl = nodes[Math.floor(Math.random() * nodes.length)].url;
    }

    const { command, clientId, sequenceNum } = await request.json();
    let result = null;

    result = await sendCommand(command, nodeUrl, clientId, sequenceNum);

    return json(result);
}

// Manage nodes: allow adding/removing nodes
export async function GET() {
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
