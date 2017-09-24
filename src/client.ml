open Protocol
open Bytes

let response_to_string = function
  | ApiVersionsResponse (correlation_id, error_code, versions) ->
      Printf.sprintf "ApiVersionsResponse [correlation_id:%d][error_code:%d][versions_size:%d]"
      correlation_id error_code (List.length versions)
  | ProduceResponse (b, c) ->
      let rec topic x acc =
        match x with
        | [] -> acc
        | (_, parts) :: xs ->
            let rec part x acc = match x with
            | [] -> acc
            | (partition, err_code, _) :: xs ->
                part xs (acc ^ ":partition:" ^ (string_of_int partition) ^ ":err_code:" ^ (string_of_int err_code)) in
            let parts = part parts "" in
            topic xs parts in
      (*let rec partitions x acc =*)
      (*  match x with*)
      (*  | [] -> acc*)
      (*  | (topic, parts) :: xs ->*)
      (*      let rec parts p acc =*)
      (*        match p with*)
      (*        | [] -> acc*)
      (*        | (part, err_code, _) :: xs ->*)
      (*            parts xs (acc ^ " : " ^ (string_of_int err_code)) in*)
      (*      partitions xs (acc ^ (parts parts "")) in*)
      Printf.sprintf "ProduceResponse [correlation_id:%d][topic_data_size:%s]"
      b (topic c "")
  | FetchResponse (correlation_id, topic) ->
      Printf.sprintf "FetchResponse [correlation_id:%d][topic:%s]"
      correlation_id topic
  | MetadataResponse (correlation_id, brokers) ->
      Printf.sprintf "MetadataResponse [correlation_id:%d][brokers_size:%d]"
      correlation_id (List.length brokers)

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

let parse_fetch_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>= fun x ->
  Lwt_io.printlf "SIZE: %d" x >>= fun _ ->
  Lwt_io.read ~count:x ic >|= fun b ->
  decode_fetch_resp b
  (*if (Bytes.length b) <= x then*)
  (*  ((Lwt_io.read ~count:x ic) >|= fun b2 ->*)
  (*    let _ = Lwt_io.printl "Doing this shit..." in*)
  (*    decode_fetch_resp (Bytes.cat b b2))*)
  (*else*)
  (*  (Lwt.return b >|= decode_fetch_resp)*)
  (*if (Bytes.length b) <= 4 then ((Lwt_io.read ~count:x ic) >|= decode_fetch_resp) else (Lwt.return b >|= decode_fetch_resp)*)
  (*Lwt_io.read ~count:x ic >|= decode_fetch_resp*)

let parse_metadata_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>= fun x ->
  Lwt_io.read ~count:x ic >|= decode_metadata_resp
  (*Lwt_io.printlf "SIZE METADATA: %d" x >>= fun _ ->*)
  (*Lwt_io.read ~count:x ic >>= fun bytes ->*)
  (*let correlation_id = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int in*)
  (*let brokers_length = (Bytes.sub bytes 4 4) |> read_int32 |> Int32.to_int in*)
  (*(*let broker0_node_id = (Bytes.sub bytes 8 4) |> read_int32 |> Int32.to_int in*)*)
  (*(*let broker0_host_length = 13 in*)*)
  (*(*let broker0_host = Bytes.sub bytes 14 broker0_host_length in*)*)
  (*(*let broker0_port = (Bytes.sub bytes 27 4) |> read_int32 |> Int32.to_int in*)*)
  (*(*let broker1_node_id = (Bytes.sub bytes 31 4) |> read_int32 |> Int32.to_int in*)*)
  (*(*let broker1_host_length = 13 in*)*)
  (*(*let broker1_host = Bytes.sub bytes 37 broker1_host_length in*)*)
  (*(*let broker1_port = (Bytes.sub bytes 50 4) |> read_int32 |> Int32.to_int in*)*)
  (*(*let broker2_node_id = (Bytes.sub bytes 54 4) |> read_int32 |> Int32.to_int in*)*)
  (*(*let broker2_host_length = 13 in*)*)
  (*(*let broker2_host = Bytes.sub bytes 60 broker2_host_length in*)*)
  (*(*let broker2_port = (Bytes.sub bytes 73 4) |> read_int32 |> Int32.to_int in*)*)
  (*let x = (Bytes.sub bytes 170 4) |> read_int32 |> Int32.to_int in *)
  (*(*Lwt_io.printlf "Correlation_id: %d\nbrokers_length: %d\nbroker0_node_id: %d\nbroker0_host: %s\nbroker0_port: %d\nbroker1_node_id: %d\nbroker1_host: %s\nbroker1_port: %d"*)*)
  (*Lwt_io.printlf "Correlation_id: %d\nbrokers_length: %d\nx: %d"*)
  (*  correlation_id brokers_length x*)
