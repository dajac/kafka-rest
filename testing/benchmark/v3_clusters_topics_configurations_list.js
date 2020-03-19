import { check } from "k6";
import { Trend, Counter } from 'k6/metrics';
import { options } from "./common.js";
import { getClusterId, createTopic, deleteTopic, updateTopicConfiguration, getTopicConfigurations } from "./api.js";

export { options };

let duration = new Trend('list_duration');
let counter = new Counter('list_count');

let topicName = "topic"

export function setup() {    
    let clusterId = getClusterId()
    createTopic(clusterId, topicName)
    updateTopicConfiguration(clusterId, topicName, "cleanup.policy", "delete")
}

export default function() {
    let clusterId = getClusterId()
    let res = getTopicConfigurations(clusterId, topicName)
    duration.add(res.timings.duration)
    counter.add(1)
    check(res, {
        "status is ok": (r) => r.status === 200
    }) 
  }

export function teardown(data) {
    let clusterId = getClusterId()
    deleteTopic(clusterId, topicName)
}