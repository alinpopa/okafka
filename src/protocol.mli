open Lib

type correlation_id = int
type client_id = string
type api_version = int
type api_key = int
type error_code = int
type api_versions = (api_key * int * int)
type req_header = (api_key * api_version * correlation_id * client_id)
type resp_header = correlation_id
type acks = int
type timeout = int
type topic = string
type partition = int
type key = bytes
type value = bytes
type record = (key * value)
type partition_data = (partition * record list)
type topic_data = (topic * partition_data list)
type offset = int64
type node_id = int
type host = string
type port = int
type broker = (node_id * host * port)
type partition_response = (partition * error_code * offset)
type topic_response = (topic * partition_response list)

type request =
  | ApiVersionsReq of req_header
  | ProduceReq of (req_header * acks * timeout * topic_data)
  | FetchReq of (req_header * topic * partition * offset)
  | MetadataReq of (req_header * topic)

type response =
  | ApiVersionsResponse of (correlation_id * error_code * api_versions list)
  | ProduceResponse of (correlation_id * topic_response list)
  | FetchResponse of (correlation_id * topic)
  | MetadataResponse of (correlation_id * broker list)

val create_api_versions_req : string -> request
val create_produce_req : client_id -> topic -> partition -> key -> value -> request
val create_fetch_req : client_id -> topic -> partition -> offset -> request
val create_metadata_req : client_id -> topic -> request

val encode_req : request -> bytes

val decode_produce_resp : bytes -> (response, string) either
val decode_api_versions_resp : bytes -> (response, string) either
val decode_fetch_resp : bytes -> (response, string) either
val decode_metadata_resp : bytes -> (response, string) either
