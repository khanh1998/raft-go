/**
 * Fetches the list of nodes from the server.
 *
 * @param {Object} context - The context object provided by SvelteKit.
 * @param {function} context.fetch - The fetch function used to make HTTP requests.
 * @returns {Promise<{ nodes: Array<any> }>} - A promise that resolves to an object containing the nodes data.
 */
export async function load({ fetch }) {
    console.log("load called")
    const response = await fetch('/api/nodes');
    const data = await response.json();
    console.log("load done", data)
    return {
        nodes: data
    };

}
