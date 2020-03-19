import { check } from "k6";
import { Trend, Counter } from 'k6/metrics';
import { options } from "./common.js";
import { getClusterId, createTopic, deleteTopic, getTopic } from "./api.js";

export { options };

let duration = new Trend('delete_duration');
let counter = new Counter('delete_count');

let topicName = "topic-get"

export default function() {
    let clusterId = getClusterId()
    let res = deleteTopic(clusterId, topicName)
    duration.add(res.timings.duration)
    counter.add(1)
    check(res, {
        "status is ok": (r) => r.status === 404
    }) 
  }
