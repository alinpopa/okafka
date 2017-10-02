open Protocol
open Okafka_lib.Bytes

let resp_api_version_to_string resp =
  let (corr_id, versions, err_code) = resp in
  Printf.(
    let a = sprintf "\t[corr_id:%d]" corr_id in
    let b = sprintf "\t[versions_size:%d]" (List.length versions) in
    let c = sprintf "\t[err_code:%d]" err_code in
    sprintf "ApiVersionsResponse\n%s\n%s\n%s"
    a b c)

let resp_produce_to_string resp =
  let (corr_id, topic_response, err_code) = resp in
  let (topic, partition, offset) = topic_response in
  Printf.(
    let a = sprintf "\t[corr_id:%d]" corr_id in
    let b = sprintf "\t[topic:%s]" topic in
    let c = sprintf "\t[partition:%d]" partition in
    let d = sprintf "\t[offset:%Ld]" offset in
    let e = sprintf "\t[err_code:%d]" err_code in
    sprintf "ProduceResponse\n%s\n%s\n%s\n%s\n%s"
    a b c d e)

let resp_fetch_to_string resp =
  let (corr_id, topic, err_code) = resp in
  Printf.(
    let a = sprintf "\t[corr_id:%d]" corr_id in
    let b = sprintf "\t[topic:%s]" topic in
    let c = sprintf "\t[err_code:%d]" err_code in
    sprintf "FetchResponse\n%s\n%s\n%s"
    a b c)

let resp_metadata_to_string resp =
  let (corr_id, brokers, part_metadata, err_code) = resp in
  let part_metadata_to_string part_metadata =
    List.fold_left (
      fun acc (part, leader, err) ->
        acc ^ (Printf.sprintf "{partition:%d}{leader:%d}{err_code:%d}" part leader err)
    ) "" part_metadata in
  Printf.(
    let a = sprintf "\t[corr_id:%d]" corr_id in
    let b = sprintf "\t[brokers_size:%d]" (List.length brokers) in
    let c = sprintf "\t[err_code:%d]" err_code in
    let d = sprintf "\t[part_metadata:%s]" (part_metadata_to_string part_metadata) in
    sprintf "MetadataResponse\n%s\n%s\n%s\n%s"
    a b c d)

let response_to_string = function
  | `ApiVersionsResp resp ->
      resp_api_version_to_string resp
  | `ProduceResp resp ->
      resp_produce_to_string resp
  | `FetchResp resp ->
      resp_fetch_to_string resp
  | `MetadataResp resp ->
      resp_metadata_to_string resp

let versions_to_string versions =
  List.fold_left (fun acc (api_key, min, max) ->
    acc ^ (Printf.sprintf "{api_key: %d, min: %d, max: %d}" api_key min max))
    ""

let response_printer resp =
  Lwt.(resp >|= response_to_string >>= fun x -> Lwt_io.printl x)

let read_bytes ic length =
  let open Lwt in
  let rec read_bytes ic length acc =
    if length <= 0 then
      Lwt.return acc
    else
      Lwt_io.read ~count:length ic >>=
      fun bytes -> read_bytes ic (length - (Bytes.length bytes)) (Bytes.cat acc bytes) in
  read_bytes ic length Bytes.empty

let parse_produce_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=
  read_bytes ic >|= decode_produce_resp

let parse_api_versions_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=
  read_bytes ic >|= decode_api_versions_resp

let parse_fetch_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=
  read_bytes ic >|= decode_fetch_resp

let parse_metadata_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=
  read_bytes ic >|= decode_metadata_resp

module Client : sig
  type t
  type 'a resp =
    | Response of 'a
    | Reconnected of (t * 'a)
  type 'a lwt = 'a resp Lwt.t
  type broker = (string * int)

  val create : broker -> t

  val send :
    t ->
    [`ProduceReq of (req_header * acks * timeout * topic_data)] ->
    [`ProduceResp of (correlation_id * topic_response * error_code)] lwt
end = struct
  type t = (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t
  type 'a resp =
    | Response of 'a
    | Reconnected of (t * 'a)
  type 'a lwt = 'a resp Lwt.t
  type broker = (string * int)

  let create (ip, port) =
    let open Lwt in
    let default_ctx = Conduit_lwt_unix.default_ctx in
    let connect ip port = (
      let connection =
        Conduit_lwt_unix.endp_to_client
        ~ctx:default_ctx
        Conduit.(`TCP ((Ipaddr.of_string_exn ip), port)) in
      let connected_client =
        connection >>=
        fun client -> Conduit_lwt_unix.connect default_ctx client in
      connected_client >|=
      (fun ((_flow, ic, oc)) -> (ic, oc))
    ) in
    connect ip port

  let send client data =
    let open Lwt in
    client >>=
    fun (ic, oc) -> Lwt_io.write oc (encode_req (data :> request)) >>=
    fun _ -> parse_produce_resp ic >|=
    fun r -> Response r
end

