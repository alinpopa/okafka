open OkafkaLib.Bytes
open OkafkaProto

let read_bytes ic length =
  let open Lwt in
  let rec read_bytes ic length acc =
    if length <= 0 then
      Lwt.return acc
    else
      Lwt_io.read ~count:length ic >>=
      fun bytes ->
        let x = Bytes.cat acc (Bytes.of_string bytes) in
        read_bytes ic (length - (String.length bytes)) x in
  read_bytes ic length Bytes.empty

(*let with_acc acc value =*)
(*  (acc, value)*)

let read_int32 =
  let open OkafkaLib.Lsm in
  get >>= fun (bytes, pos) ->
  put (bytes, pos + 4) >>= fun _ ->
  return (Bytes.sub bytes pos 4 |> read_int32_to_int)

let read_int16 =
  let open OkafkaLib.Lsm in
  state (fun (bytes, pos) ->
    ((Bytes.sub bytes pos 2) |> read_int16_to_int, (bytes, pos + 2)))

let read_int64 =
  let open OkafkaLib.Lsm in
  state (fun (bytes, pos) ->
    (Bytes.sub bytes pos 8 |> read_int64, (bytes, pos + 8)))

let read_int8 =
  let open OkafkaLib.Lsm in
  state (fun (bytes, pos) ->
    ((Bytes.sub bytes pos 1) |> read_int8 |> Int8.to_int, (bytes, pos + 1)))

let read_data length =
  let open OkafkaLib.Lsm in
  state (fun (bytes, pos) ->
    (Bytes.sub bytes pos length, (bytes, pos + length)))

let decode_metadata_resp bytes =
  let open Proto in
  let open OkafkaLib.Lsm in
  let (r, _) = run (
    read_int32 >>= fun correlation_id ->
    read_int32 >>= fun brokers_size ->
      let rec brokers n acc =
        if n = 0 then
          return acc
        else
          read_int32 >>= fun node_id ->
          read_int16 >>= fun host_length ->
          read_data host_length >>= fun host ->
          read_int32 >>= fun port ->
          let host = Bytes.to_string host in
          brokers (n - 1) ({node_id; host; port} :: acc) in
      brokers brokers_size [] >>= fun brokers ->
    (*modify (fun (x, i) -> (x, i + 4)) >>> fun _ ->*)
    read_int32 >>= fun _topic_metadata_length ->
    read_int16 >>= fun error_code ->
    read_int16 >>= read_data >>= fun _topic ->
    (*read_data topic_length >>> fun _topic ->*)
    read_int32 >>= fun part_metadata_size ->
      let rec part_metadata n acc =
        if n = 0 then
          return acc
        else
          read_int16 >>= fun error_code ->
          read_int32 >>= fun partition ->
          read_int32 >>= fun leader ->
          read_int32 >>= fun replica_size ->
          (* read all replicas, each replica has 4 bytes, hence (size * 4) *)
          read_data (replica_size * 4) >>= fun _replicas ->
          read_int32 >>= fun isr_size ->
          (* read all isrs, each isr has 4 bytes, hence (size * 4) *)
          read_data (isr_size * 4) >>= fun _isr ->
          part_metadata (n - 1) ({partition; leader; error_code} :: acc) in
      part_metadata part_metadata_size [] >>= fun leaders ->
      return (Resp.({
        Metadata.correlation_id;
        brokers;
        leaders;
        error_code
      }))) (bytes, 0) in
  r

let decode_produce_resp bytes =
  let open Proto in
  let open OkafkaLib.Lsm in
  let (r, _) = run (
    read_int32 >>= fun correlation_id ->
    read_int32 >>= fun _topic_response_size ->
    read_int16 >>= fun topic_length ->
    read_data topic_length >>= fun topic ->
    read_int32 >>= fun _partitions_resp_size ->
    read_int32 >>= fun partition ->
    read_int16 >>= fun error_code ->
    read_int64 >>= fun offset ->
    let topic = Bytes.to_string topic in
    let topic_response = {topic; partition; offset} in
    return (Resp.({
      Produce.correlation_id;
      topic_response;
      error_code
    }))) (bytes, 0) in
  r

let decode_fetch_resp bytes =
  let open Proto in
  let open OkafkaLib.Lsm in
  let (r, _) = run (
    read_int32 >>= fun correlation_id ->
    read_int32 >>= fun _responses_length ->
    read_int16 >>= fun topic_length ->
    read_data topic_length >>= fun topic ->
    read_int32 >>= fun _partitions_length ->
    read_int32 >>= fun partition ->
    read_int16 >>= fun error_code ->
    read_int64 >>= fun _high_watermark ->
    read_int32 >>= fun record_set_length ->
      let rec collect_records n acc =
        if n <= 0 then
          return acc
        else
          get >>= fun (_, start_pos) ->
          read_int64 >>= fun offset ->
          read_int32 >>= fun _message_size ->
          read_int32 >>= fun _crc ->
          read_int8 >>= fun _magic_byte ->
          read_int8 >>= fun _attr ->
          read_int32 >>= fun key_length ->
          read_data key_length >>= fun key ->
          read_int32 >>= fun val_length ->
          read_data val_length >>= fun value ->
          get >>= fun (_, end_pos) ->
          collect_records (n - (end_pos - start_pos)) ((offset, {key; value}) :: acc) in
      collect_records record_set_length [] >>= fun data ->
      let topic = Bytes.to_string topic in
      return (Resp.({
        Fetch.correlation_id;
        topic;
        error_code;
        partition;
        data
      }))) (bytes, 0) in
  r

let parse_metadata_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= Bytes.of_string >|= read_int32_to_int >>=
  read_bytes ic >|= decode_metadata_resp

let parse_produce_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= Bytes.of_string >|= read_int32_to_int >>=
  read_bytes ic >|= decode_produce_resp

let parse_fetch_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= Bytes.of_string >|= read_int32_to_int >>=
  read_bytes ic >|= decode_fetch_resp
