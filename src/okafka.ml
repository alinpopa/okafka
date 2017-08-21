type kafka_correlation_id = int
type kafka_client_id = string
type kafka_api_version = int
type kafka_api_key = int
type kafka_error_code = int
type kafka_api_versions = (kafka_api_key * int * int)
type kafka_req_header = (kafka_api_key * kafka_api_version * kafka_correlation_id * kafka_client_id)
type kafka_resp_header = kafka_correlation_id

type kafka_request =
  | ApiVersionsReq of kafka_req_header

type kafka_response =
  | ApiVersionsResponse of (kafka_correlation_id * kafka_error_code * kafka_api_versions list)

let write_4_bytes (i : int) =
  [
    (i lsr 24) land 0xFF |> char_of_int;
    (i lsr 16) land 0xFF |> char_of_int;
    (i lsr 8) land 0xFF |> char_of_int;
    (i lsr 0) land 0xFF |> char_of_int
  ]

let write_2_bytes (i : int) =
  [
    (i lsr 8) land 0xFF |> char_of_int;
    (i lsr 0) land 0xFF |> char_of_int
  ]

let to_buffer data =
  let n = List.length data in
  let buffer = Bytes.create n in
  let _ = List.fold_left
    (fun acc e -> let _ = Bytes.set buffer acc e in (acc + 1))
    0 data in
  Bytes.copy buffer

let req_to_buffer = function
  | ApiVersionsReq (api_key, api_version, correlation_id, client_id) ->
      let client_id_length = String.length client_id in
      let bytes_length = 10 + client_id_length in
      to_buffer (List.flatten [
        write_4_bytes bytes_length;
        write_2_bytes api_key;
        write_2_bytes api_version;
        write_4_bytes correlation_id;
        write_2_bytes client_id_length;
        Core.String.to_list client_id
      ])

let kafka_api_versions_req () =
  let kafka_req = ApiVersionsReq (18, 0, 1, "test123") in
  req_to_buffer kafka_req

let read_int32 buffer =
  let a = (Bytes.get buffer 0 |> int_of_char) lsl 24 in
  let b = (Bytes.get buffer 1 |> int_of_char) lsl 16 in
  let c = (Bytes.get buffer 2 |> int_of_char) lsl 8 in
  let d = (Bytes.get buffer 3 |> int_of_char) lsl 0 in
  a lor b lor c lor d

let read_int16 buffer =
  let a = (Bytes.get buffer 0 |> int_of_char) lsl 8 in
  let b = (Bytes.get buffer 1 |> int_of_char) lsl 0 in
  a lor b

let rec read_versions n versions ic =
  let open Lwt in
  if n = 0 then
    return versions
  else
    Lwt_io.read ~count:2 ic >|= read_int16 >>= fun api_key ->
    Lwt_io.read ~count:2 ic >|= read_int16 >>= fun min_version ->
    Lwt_io.read ~count:2 ic >|= read_int16 >>= fun max_version ->
    read_versions (n - 1) ((api_key, min_version, max_version) :: versions) ic

let response_parser ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >>= fun req_length ->
  Lwt_io.read ~count:4 ic >|= read_int32 >>= fun correlation_id ->
  Lwt_io.read ~count:2 ic >|= read_int16 >>= fun error_code ->
  Lwt_io.read ~count:4 ic >|= read_int32 >>= fun api_versions_length ->
  read_versions api_versions_length [] ic >|= fun versions ->
  ApiVersionsResponse (correlation_id, error_code, versions) 

let response_to_string = function
  | ApiVersionsResponse (correlation_id, error_code, _) ->
      Printf.sprintf "ApiVersionsResponse [correlation_id:%d][error_code:%d]"
      correlation_id error_code

let versions_to_string versions =
  List.fold_left (fun acc (api_key, min, max) ->
    acc ^ (Printf.sprintf "{api_key: %d, min: %d, max: %d}" api_key min max))
    ""
let response_printer resp =
  Lwt.(resp >|= response_to_string >>= Lwt_io.printl)
