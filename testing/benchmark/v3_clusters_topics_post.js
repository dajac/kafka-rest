import { check } from "k6";
import { Trend, Counter } from 'k6/metrics';
import { options } from "./common.js";
import { getClusterId, createTopic, deleteTopic, listTopics } from "./api.js";

export { options };

let duration = new Trend('create_duration');
let counter = new Counter('create_count');

let topicName = "topic-get"
let cnt = 0

export default function() {
    let clusterId = getClusterId()
    let res = createTopic(clusterId, `${topicName}-${__VU}-${cnt}`)
    duration.add(res.timings.duration)
    counter.add(1)
    cnt++
    check(res, {
        "status is ok": (r) => r.status === 201
    }) 
  }
