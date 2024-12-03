/**
 * @typedef {Object} Node
 * @property {string} url
 * @property {number} id
 * @property {string} state
 * @property {number} term
 * @property {number} leader_id
 * @property {number} cluster_time
 */


/**
 * @enum {string}
 */
const State = {
    leader: 'leader',
    follower: 'follower',
    candidate: 'candidate',
};

export { }; // Ensure this file is recognized as a module.
