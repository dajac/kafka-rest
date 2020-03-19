import { check } from "k6";
import { Trend, Counter } from 'k6/metrics';
import { options } from "./common.js";
import { getClusterId, createTopic, deleteTopic, listTopics } from "./api.js";

export { options };

let duration = new Trend('list_duration');
let counter = new Counter('list_count');

let nbTopics = 100

export function setup() {    
    let clusterId = getClusterId()

    for (let i = 0; i < nbTopics; i++) {
        createTopic(clusterId, `topic-${i}`)
    }
}

export default function() {
    let clusterId = getClusterId()
    let res = listTopics(clusterId)
    duration.add(res.timings.duration)
    counter.add(1)
    check(res, {
        "status is ok": (r) => r.status === 200
    }) 
  }

export function teardown(data) {
    let clusterId = getClusterId()

    for (let i = 0; i < nbTopics; i++) {
        deleteTopic(clusterId, `topic-${i}`)
    }
}