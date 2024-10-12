<!-- <script context="module">
    export async function load({ fetch }) {
        const res = await fetch("/api/nodes");
        const nodes = await res.json();
        return { props: { nodes } };
    }
</script> -->

<script>
    export let data;
    /**
     * @type {any[]}
     */
    let nodes = data.nodes;

    /**
     * @type {number}
     */
    let clientId = 0;
    let sequenceNum = 0;
    let command = "";
    let key = "";
    let value = "";
    let waitingForApi = false;
    /**
     * @type {any | null}
     */
    let result = null;

    $: registerRequired = clientId == 0;

    async function executeCommand() {
        let res, cmd;
        switch (command) {
            case "register":
                waitingForApi = true;
                res = await fetch("/api/nodes", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ command, clientId, sequenceNum }),
                });
                result = await res.json();
                clientId = result?.response;
                sequenceNum = 1;
                waitingForApi = false;
                break;
            case "get":
                waitingForApi = true;
                cmd = command + " " + key;
                res = await fetch("/api/nodes", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ command: cmd, clientId, sequenceNum }),
                });
                result = await res.json();
                waitingForApi = false;
                break;
            case "set":
                waitingForApi = true;
                sequenceNum += 1;
                cmd = command + " " + key + " " + value;
                res = await fetch("/api/nodes", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ command: cmd, clientId, sequenceNum }),
                });
                result = await res.json();
                waitingForApi = false;
                break;
            default:
                result = "Unknown command";
                waitingForApi = false;
                break;
        }
    }
</script>

<div>
    <h1>Raft Static Cluster</h1>
    <ol>
        {#each nodes as node, index}
            <li class="node-item">
                <span>{node.url}</span>
                <div class="node-controls">
                    {#if node.leader}
                        <strong>(Leader)</strong>
                    {/if}
                </div>
            </li>
        {/each}
    </ol>

    <div>
        Client ID: <b>{clientId}</b>
        Sequence Num: <b>{sequenceNum}</b>
    </div>

    <!-- Command Input -->
    <div class="command-input">
        <label>Command:</label>
        <select bind:value={command}>
            <option value="register">Register (Get Session ID)</option>
            <option value="get" hidden="{registerRequired}">Get Value</option>
            <option value="set" hidden="{registerRequired}">Set Value</option>
        </select>

        {#if command === "get" || command === "set"}
            <input type="text" bind:value={key} placeholder="Key" />
        {/if}

        {#if command === "set"}
            <input type="text" bind:value placeholder="Value" />
        {/if}

        <button on:click={executeCommand}>Send Command</button>
    </div>

    <!-- Display Result -->
    <div>
        {#if !waitingForApi}
            <h3>Result:</h3>
            <p>{JSON.stringify(result)}</p>
        {:else}
            <h3>Loading...</h3>
        {/if}
    </div>
</div>

<style>
    /* Basic styling */
    .node-list,
    .command-input {
        margin-bottom: 10px;
    }

    .node-item {
        display: flex;
        justify-content: space-between;
        margin-bottom: 5px;
    }

    .node-controls {
        margin-left: 10px;
    }
</style>
