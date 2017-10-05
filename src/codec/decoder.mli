open OkafkaProto

val parse_metadata_resp :
  Lwt_io.input_channel ->
  Resp.Metadata.t Lwt.t 

val parse_produce_resp :
  Lwt_io.input_channel ->
  Resp.Produce.t Lwt.t

val parse_fetch_resp :
  Lwt_io.input_channel ->
  Resp.Fetch.t Lwt.t 
