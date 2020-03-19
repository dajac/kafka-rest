import { check } from "k6";
import { Trend, Counter } from 'k6/metrics';
import { options } from "./common.js";
import { getClusterId, createTopic, deleteTopic, getTopic } from "./api.js";

export { options };

let duration = new Trend('create_duration');
let counter = new Counter('create_count');

let topicName = "topic-get"

export function setup() {    
    let clusterId = getClusterId()
    createTopic(clusterId, topicName)
}

export default function() {
    let clusterId = getClusterId()
    let res = createTopic(clusterId, topicName)
    duration.add(res.timings.duration)
    counter.add(1)
    check(res, {
        "status is ok": (r) => r.status === 400
    }) 
  }

export function teardown(data) {
    let clusterId = getClusterId()
    deleteTopic(clusterId, topicName)
}