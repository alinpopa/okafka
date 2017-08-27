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

type request =
  | ApiVersionsReq of req_header
  | ProduceReq of (req_header * acks * timeout * topic_data)

type response =
  | ApiVersionsResponse of (correlation_id * error_code * api_versions list)
  | HeadersResponse of (correlation_id * error_code)
  | ProduceResponse of (correlation_id * int)

val create_api_versions_req : string -> request
val create_produce_req : client_id -> topic -> partition -> key -> value -> request

val encode_req : request -> bytes

val decode_produce_resp : bytes -> (response, string) either
val decode_api_versions_resp : bytes -> (response, string) either
