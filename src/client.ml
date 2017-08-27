open Protocol
open Bytes

let response_to_string = function
  | ApiVersionsResponse (correlation_id, error_code, versions) ->
      Printf.sprintf "ApiVersionsResponse [correlation_id:%d][error_code:%d][versions_size:%d]"
      correlation_id error_code (List.length versions)
  | HeadersResponse (correlation_id, error_code) ->
      Printf.sprintf "HeadersResponse [correlation_id:%d][error_code:%d]"
      correlation_id error_code
  | ProduceResponse (b, c) ->
      Printf.sprintf "ProduceResponse [correlation_id:%d][topic_data_size:%d]"
      b c

let versions_to_string versions =
  List.fold_left (fun acc (api_key, min, max) ->
    acc ^ (Printf.sprintf "{api_key: %d, min: %d, max: %d}" api_key min max))
    ""
let response_printer resp =
  Lwt.(resp >|= response_to_string >>= fun x -> Lwt_io.printl x)

let parse_produce_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>= fun x ->
  Lwt_io.read ~count:x ic >|= decode_produce_resp

let parse_api_versions_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>= fun x ->
  Lwt_io.read ~count:x ic >|= decode_api_versions_resp
