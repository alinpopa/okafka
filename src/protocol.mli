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
type topic_response = (topic * partition * offset)
type leader = node_id
type partition_metadata = (partition * leader * error_code)

type request = [
  | `ApiVersionsReq of req_header
  | `ProduceReq of (req_header * acks * timeout * topic_data)
  | `FetchReq of (req_header * topic * partition * offset)
  | `MetadataReq of (req_header * topic)
]

type response = [
  | `ApiVersionsResp of (correlation_id * api_versions list * error_code)
  | `ProduceResp of (correlation_id * topic_response * error_code)
  | `FetchResp of (correlation_id * topic * error_code)
  | `MetadataResp of (correlation_id * broker list * partition_metadata list * error_code)
]

type ('a, 'b) client_response =
  | Just of 'b
  | Retry of ('a * broker)
  | Failed of string

val create_api_versions_req :
  string ->
  [`ApiVersionsReq of req_header]
val create_produce_req :
  client_id ->
  topic ->
  partition ->
  key ->
  value ->
  [`ProduceReq of (req_header * acks * timeout * topic_data)]
val create_fetch_req :
  client_id ->
  topic ->
  partition ->
  offset ->
  [`FetchReq of (req_header * topic * partition * offset)]
val create_metadata_req :
  client_id ->
  topic ->
  [`MetadataReq of (req_header * topic)]

val encode_req : request -> bytes

val decode_produce_resp :
  bytes ->
  [`ProduceResp of (correlation_id * topic_response * error_code)]
val decode_api_versions_resp :
  bytes ->
  [`ApiVersionsResp of (correlation_id * api_versions list * error_code)]
val decode_fetch_resp :
  bytes ->
  [`FetchResp of (correlation_id * topic * error_code)]
val decode_metadata_resp :
  bytes ->
  [`MetadataResp of (correlation_id * broker list * partition_metadata list * error_code)]
