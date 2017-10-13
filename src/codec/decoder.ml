open OkafkaLib.Bytes
open OkafkaProto

let read_bytes ic length =
  let open Lwt in
  let rec read_bytes ic length acc =
    if length <= 0 then
      Lwt.return acc
    else
      Lwt_io.read ~count:length ic >>=
      fun bytes -> read_bytes ic (length - (Bytes.length bytes)) (Bytes.cat acc bytes) in
  read_bytes ic length Bytes.empty

let with_acc acc value =
  (acc, value)

let decode_metadata_resp bytes =
  let open Proto in
  let (pos, correlation_id) = (Bytes.sub bytes 0 4) |> read_int32_to_int |> with_acc 4 in
  let (pos, brokers_size) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let rec brokers n _pos acc =
    if n = 0 then
      (_pos, acc)
    else (
      let pos = _pos in
      let (pos, node_id) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
      let (pos, host_length) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
      let (pos, host) = Bytes.sub bytes pos host_length |> with_acc (pos + host_length) in
      let (pos, port) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
      brokers (n - 1) pos ({node_id; host; port} :: acc)
    ) in
  let (pos, brokers) = brokers brokers_size 8 [] in
  let (pos, _topic_metadata_length) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
  let (pos, _topic_length) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
  let (pos, _topic) = (Bytes.sub bytes pos _topic_length) |> with_acc (pos + _topic_length) in
  let (pos, part_metadata_size) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let rec part_metadata n _pos acc =
    if n = 0 then
      (_pos, acc)
    else (
      let pos = _pos in
      let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
      let (pos, partition) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
      let (pos, leader) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
      let (pos, replica_size) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
      let (pos, _replicas) = (Bytes.sub bytes pos (replica_size * 4)) |> read_int32_to_int |> with_acc (pos + (replica_size * 4)) in
      let (pos, isr_size) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
      let (pos, _isr) = (Bytes.sub bytes pos (isr_size * 4)) |> read_int32_to_int |> with_acc (pos + (isr_size * 4)) in
      part_metadata (n - 1) pos ({partition; leader; error_code} :: acc)
    ) in
  let (_pos, leaders) = part_metadata part_metadata_size pos [] in
  Resp.({
    Metadata.correlation_id;
    brokers;
    leaders;
    error_code
  })

let decode_produce_resp bytes =
  let open Proto in
  let (pos, correlation_id) = (Bytes.sub bytes 0 4) |> read_int32_to_int |> with_acc 4 in
  let (pos, _topic_response_size) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, topic_length) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
  let (pos, topic) = (Bytes.sub bytes pos topic_length) |> with_acc (pos + topic_length) in
  let (pos, _partitions_resp_size) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, partition) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
  let (_pos, offset) = (Bytes.sub bytes pos 8) |> read_int64 |> with_acc (pos + 8) in
  let topic_response = {topic; partition; offset} in
  Resp.({
    Produce.correlation_id;
    topic_response;
    error_code
  })

let decode_fetch_resp bytes =
  let open Proto in
  let (pos, correlation_id) = (Bytes.sub bytes 0 4) |> read_int32_to_int |> with_acc 4 in
  let (pos, _responses_length) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, topic_length) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
  let (pos, topic) = (Bytes.sub bytes pos topic_length) |> with_acc (pos + topic_length) in
  let (pos, _partitions_length) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, partition) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16_to_int |> with_acc (pos + 2) in
  let (pos, _high_watermark) = (Bytes.sub bytes pos 8) |> read_int64 |> with_acc (pos + 8) in
  let (pos, record_set_length) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) in
  let rec collect_records n pos acc =
    if n <= 0 then
      (pos, acc)
    else
      let (a, (pos, offset)) = (Bytes.sub bytes pos 8) |> read_int64 |> with_acc (pos + 8) |> with_acc 8 in
      let (a, (pos, message_size)) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) |> with_acc (a + 4) in
      let (a, (pos, crc)) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) |> with_acc (a + 4) in
      let (a, (pos, magic_byte)) = (Bytes.sub bytes pos 1) |> read_int8 |> Int8.to_int |> with_acc (pos + 1) |> with_acc (a + 1) in
      let (a, (pos, attr)) = (Bytes.sub bytes pos 1) |> read_int8 |> Int8.to_int |> with_acc (pos + 1) |> with_acc (a + 1) in
      let (a, (pos, key_length)) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) |> with_acc (a + 4) in
      let (a, (pos, key)) = (Bytes.sub bytes pos key_length) |> with_acc (pos + key_length) |> with_acc (a + key_length) in
      let (a, (pos, val_length)) = (Bytes.sub bytes pos 4) |> read_int32_to_int |> with_acc (pos + 4) |> with_acc (a + 4) in
      let (a, (pos, value)) = (Bytes.sub bytes pos val_length) |> with_acc (pos + val_length) |> with_acc (a + val_length) in
      collect_records (n - a) pos ((offset, {key; value}) :: acc) in
  let (pos, data) = collect_records record_set_length pos [] in
  Resp.({
    Fetch.correlation_id;
    topic;
    error_code;
    partition;
    data
  })

let parse_metadata_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32_to_int >>=
  read_bytes ic >|= decode_metadata_resp

let parse_produce_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32_to_int >>=
  read_bytes ic >|= decode_produce_resp

let parse_fetch_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32_to_int >>=
  read_bytes ic >|= decode_fetch_resp
