import { check } from "k6";
import { Trend, Counter } from 'k6/metrics';
import { options } from "./common.js";
import { getClusterId, createTopic, deleteTopic, getTopicPartition } from "./api.js";

export { options };

let duration = new Trend('get_duration');
let counter = new Counter('get_count');

export function setup() {    
    let clusterId = getClusterId()
    createTopic(clusterId, "topic-1", "100", "3")
}

export default function() {
    let clusterId = getClusterId()
    let res = getTopicPartition(clusterId, "topic-1", 0)
    duration.add(res.timings.duration)
    counter.add(1)
    check(res, {
        "status is ok": (r) => r.status === 200
    }) 
  }

export function teardown(data) {
    let clusterId = getClusterId()
    deleteTopic(clusterId, "topic-1")
}