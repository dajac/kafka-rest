import http from "k6/http";
import { urlbase } from "./common.js";

let clusterId = null

export function getClusterId() {
    if (clusterId === null) {
        let url = urlbase + `/v3/clusters`
        let res = http.get(url)
        clusterId = JSON.parse(res.body)["data"][0]["attributes"]["cluster_id"]
    }
    return clusterId
}

export function createTopic(clusterId, topicName, partitionCount = "3", replicationFactor = "1") {
    let url = urlbase + `/v3/clusters/${clusterId}/topics`
    var params = {
        headers: {
          'Accept': 'application/vnd.api+json',
        },
    };
    let payload = JSON.stringify({
        data: {
            attributes: {
                topic_name: topicName,
                partitions_count: partitionCount,
                replication_factor: replicationFactor,
            }
        }
    });
    return http.post(url, payload, params);
}

export function deleteTopic(clusterId, topicName) { 
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}`
    var params = {
        headers: {
          'Accept': 'application/vnd.api+json',
        },
    };
    return http.del(url, null, params);
}

export function listTopics(clusterId) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics`
    return http.get(url);
}

export function getTopic(clusterId, topicName) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}`
    return http.get(url);
}

export function listTopicPartitions(clusterId, topicName) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/partitions`
    return http.get(url);
}

export function getTopicPartition(clusterId, topicName, partitionIndex) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionIndex}`
    return http.get(url);
}

export function listTopicPartitionReplicas(clusterId, topicName, partitionIndex) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionIndex}/replicas`
    return http.get(url);
}

export function getTopicPartitionReplica(clusterId, topicName, partitionIndex, brokerId) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/partitions/${partitionIndex}/replicas/${brokerId}`
    return http.get(url);
}

export function getTopicConfigurations(clusterId, topicName) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/configurations`
    return http.get(url);
}

export function getTopicConfiguration(clusterId, topicName, name) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/configurations/${name}`
    return http.get(url);
}

export function updateTopicConfiguration(clusterId, topicName, name, value) {
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/configurations/${name}`
    var params = {
        headers: {
          'Accept': 'application/vnd.api+json',
        },
    };
    let payload = JSON.stringify({
        data: {
            attributes: {
                value: value,
            }
        }
    });
    return http.put(url, payload, params);
}

export function deleteTopicConfiguration(clusterId, topicName, name) { 
    let url = urlbase + `/v3/clusters/${clusterId}/topics/${topicName}/configurations/${name}`
    var params = {
        headers: {
          'Accept': 'application/vnd.api+json',
        },
    };
    return http.del(url, null, params);
}
