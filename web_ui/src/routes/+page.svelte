<!-- <script context="module">
    export async function load({ fetch }) {
        const res = await fetch("/api/nodes");
        const nodes = await res.json();
        return { props: { nodes } };
    }
</script> -->

<script>
    import { onMount } from "svelte";
    import Cookies from "js-cookie";
    export let data;

    /**
     * @type {import('../type').Node[]}
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
    let keepSessionAlive = true;

    /**
     *
     * @param {number} clientIdValue
     */
    function setClientId(clientIdValue) {
        clientId = clientIdValue;
        Cookies.set("clientId", clientId, { expires: 1 });
    }

    /**
     *
     * @param {number} sequenceNumValue
     */
    function setSequenceNum(sequenceNumValue) {
        sequenceNum = sequenceNumValue;
        Cookies.set("sequenceNum", sequenceNum, { expires: 1 });
    }

    $: registerRequired = clientId == 0;
    $: leaderFounded = nodes.some((node) => node.state == "leader");

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
                setClientId(result?.response);
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
                    body: JSON.stringify({
                        command: cmd,
                        clientId,
                        sequenceNum,
                    }),
                });
                result = await res.json();
                waitingForApi = false;
                break;
            case "set":
                waitingForApi = true;
                setSequenceNum(sequenceNum + 1);
                cmd = command + " " + key + " " + value;
                res = await fetch("/api/nodes", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        command: cmd,
                        clientId,
                        sequenceNum,
                    }),
                });
                result = await res.json();
                waitingForApi = false;
                break;
            case "del":
                waitingForApi = true;
                setSequenceNum(sequenceNum + 1);
                cmd = command + " " + key;
                res = await fetch("/api/nodes", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        command: cmd,
                        clientId,
                        sequenceNum,
                    }),
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

    async function sendKeepSessionAlive() {
        if (!keepSessionAlive) {
            return;
        }

        if (clientId == 0) {
            console.log("there is no session to keep alive");
            return;
        }

        sequenceNum += 1;
        let cmd = "keep-alive";

        waitingForApi = true;
        let res = await fetch("/api/nodes", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ command: cmd, clientId, sequenceNum }),
        });
        result = await res.json();
        waitingForApi = false;
        console.log({ result });
    }

    async function fetchNodeInfo() {
        const response = await fetch("/api/nodes");
        const data = await response.json();
        nodes = data;
    }

    onMount(() => {
        const savedClientId = Cookies.get("clientId");
        if (savedClientId) {
            clientId = Number(savedClientId);
        }

        const savedSequenceNum = Cookies.get("sequenceNum");
        if (savedSequenceNum) {
            sequenceNum = Number(savedSequenceNum);
        }

        const intervalId = setInterval(sendKeepSessionAlive, 60000);
        const intervalId1 = setInterval(fetchNodeInfo, 30000);

        // Clean up interval when component is destroyed
        return () => {
            clearInterval(intervalId)
            clearInterval(intervalId1)
        };
    });

    function clearSession() {
        setClientId(0);
        setSequenceNum(0);
    }

    /**
     *
     * @param {number} nanoseconds
     */
    function formatClusterTime(nanoseconds) {
        const seconds = nanoseconds / 1e9;
        if (seconds < 60) {
            return `${seconds.toFixed(2)} seconds`;
        } else {
            const minutes = seconds / 60;
            return `${minutes.toFixed(2)} minutes`;
        }
    }
</script>

<div class="node-list">
    <h2>Raft Nodes</h2>

    <div class="nodes-grid">
        {#each nodes as node}
            <div class="node-card">
                <h3>Node {node.id}</h3>
                <p><strong>URL:</strong> {node.url}</p>
                <p>
                    <strong>State:</strong>
                    <span class="state {node.state}">{node.state}</span>
                </p>
                <p><strong>Term:</strong> {node.term}</p>
                <p><strong>Leader ID:</strong> {node.leader_id}</p>
                <p>
                    <strong>Cluster Time:</strong>
                    {formatClusterTime(node.cluster_time)}
                </p>
            </div>
        {/each}
    </div>

    <div>
        {#if !leaderFounded}
            <b class="warning">Cluster isn't ready,</b>
            <p>Reload page until cluster has leader</p>
        {/if}
    </div>

    <!-- Session Info and Command Input -->
    <div class="session-command-container">
        <div class="session-info">
            <h3>Session Info</h3>
            <p>Client ID: <b>{clientId}</b></p>
            <p>Sequence Num: <b>{sequenceNum}</b></p>
            <div class="session-controls">
                <label>
                    Keep session alive:
                    <input type="checkbox" bind:checked={keepSessionAlive} />
                </label>
                <button on:click={clearSession}>Clear Session</button>
            </div>
        </div>

        <!-- Command Input -->
        <div class="command-input">
            <h3>Command Input</h3>
            <label>Command:</label>
            <select bind:value={command} class="command-select">
                <option value="register">Register (Get Session ID)</option>
                <option value="get" hidden={registerRequired}>Get Value</option>
                <option value="set" hidden={registerRequired}>Set Value</option>
                <option value="del" hidden={registerRequired}
                    >Delete Value</option
                >
            </select>

            {#if command === "get" || command === "set" || command === "del"}
                <input
                    type="text"
                    bind:value={key}
                    placeholder="Key"
                    class="command-field"
                />
            {/if}

            {#if command === "set"}
                <input
                    type="text"
                    bind:value
                    placeholder="Value"
                    class="command-field"
                />
            {/if}

            <button on:click={executeCommand}>Send Command</button>
        </div>
    </div>

    <!-- Display Result -->
    <div class="result-display">
        {#if !waitingForApi}
            <h3>Result:</h3>
            <pre>{JSON.stringify(result, null, 2)}</pre>
        {:else}
            <h3>Loading...</h3>
        {/if}
    </div>
</div>

<style>
    /* Main node list styles */
    .node-list {
        max-width: 1100px;
        margin: 30px auto;
        padding: 0 15px;
        border-radius: 12px;
        background-color: #f7f9fc; /* Light background for container */
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }

    h2 {
        text-align: center;
        font-size: 1.8rem;
        margin-bottom: 30px;
        color: #0077b6; /* Primary blue */
        border-bottom: 2px solid #e76f51; /* Accent underline */
        padding-bottom: 10px;
    }

    /* Node cards grid */
    .nodes-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 15px;
        padding: 15px;
    }

    /* Node card styles */
    .node-card {
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        padding: 15px;
        transition:
            transform 0.2s ease,
            box-shadow 0.2s ease;
        border-left: 5px solid #0077b6; /* Blue accent on the left */
    }

    .node-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.15);
    }

    .node-card h3 {
        font-size: 1.4rem;
        margin-bottom: 12px;
        color: #005f8a;
    }

    .node-card p {
        margin: 8px 0;
        color: #333;
    }

    .state {
        font-weight: bold;
        padding: 3px 8px;
        border-radius: 5px;
    }

    .state.leader {
        background-color: #2a9d8f; /* Leader: green */
        color: white;
    }

    .state.follower {
        background-color: #e76f51; /* Follower: orange */
        color: white;
    }

    .state.unavailable {
        background-color: #6d13d4; /* Follower: orange */
        color: white;
    }

    .state.candidate {
        background-color: #2d8ad5; /* Follower: orange */
        color: white;
    }

    .session-command-container {
        display: flex;
        justify-content: space-between;
        margin: 20px 0;
        padding: 12px;
        border-radius: 10px;
        background-color: #f0f4f8; /* Light grey background for the section */
    }

    .session-info,
    .command-input {
        padding: 12px;
        background-color: #eaf4fc; /* Light blue background for sections */
        border: 1px solid #b7e3e0; /* Light teal border */
        border-radius: 8px;
        width: 48%;
    }

    .session-info h3,
    .command-input h3 {
        margin-top: 0;
        color: #0077b6;
    }

    .session-info p {
        margin: 5px 0;
    }

    .session-controls label {
        color: #555;
    }

    .command-input label {
        color: #0077b6;
    }

    .command-select,
    .command-field {
        width: 100%; /* Ensure the input field takes the full width of the container */
        box-sizing: border-box; /* Include padding and border in the element's total width */
        padding: 8px;
        margin-top: 2px;
        border: 1px solid #ccc;
        border-radius: 4px;
    }

    .command-input button {
        padding: 10px 15px;
        background-color: #2a9d8f; /* Green button */
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        transition: background-color 0.3s ease;
        width: 100%;
        margin-top: 10px;
    }

    .command-input button:hover {
        background-color: #21867a; /* Darker green on hover */
    }

    .warning {
        color: #e76f51;
    }

    /* Result display styling */
    .result-display {
        margin-top: 10px;
        padding: 15px;
        background-color: #f0f8f8;
        border: 1px solid #b7e3e0;
        border-radius: 8px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
    }

    .result-display h3 {
        margin-top: 0;
        color: #0077b6;
    }

    /* Responsive */
    @media (max-width: 768px) {
        .session-command-container {
            flex-direction: column;
        }

        .session-info,
        .command-input {
            width: 100%;
            margin-bottom: 15px;
        }
    }
</style>
