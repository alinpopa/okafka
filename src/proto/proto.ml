open Bytes

type correlation_id = int
type client_id = string
type api_version = int
type api_key = int
type error_code = int
type api_versions = {
  key: api_key;
  min_ver: int;
  max_ver: int
}
type req_header = {
  api_key: api_key;
  api_version: api_version;
  correlation_id: correlation_id;
  client_id: client_id
}
type resp_header = correlation_id
type acks = int
type timeout = int
type topic = string
type partition = int
type key = bytes
type value = bytes
type record = {
  key: key;
  value: value
}
type partition_data = {
  partition: partition;
  records: record list
}
type topic_data = {
  topic: topic;
  partitions_data: partition_data list
}
type offset = int64
type node_id = int
type host = string
type port = int
type broker = {
  node_id: node_id;
  host: host;
  port: port
}
type topic_response = {
  topic: topic;
  partition: partition;
  offset: offset
}
type partition_metadata = {
  partition: partition;
  leader: node_id;
  error_code: error_code
}
