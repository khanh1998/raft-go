import '../type'
/**
 * Fetches the list of nodes from the server.
 *
 * @param {Object} context - The context object provided by SvelteKit.
 * @param {function} context.fetch - The fetch function used to make HTTP requests.
 * @returns {Promise<{ nodes: Array<import('../type').Node> }>} - A promise that resolves to an object containing the nodes data.
 */
export async function load({ fetch }) {
    const response = await fetch('/api/nodes');
    const data = await response.json();
    return {
        nodes: data
    };

}
