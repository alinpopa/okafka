include OkafkaLib.Bytes

type correlation_id = int
type client_id = string
type api_version = int
type api_key = int
type error_code = int
type api_versions = {
  key: api_key;
  min_ver: int;
  max_ver: int
}
type req_header = {
  api_key: api_key;
  api_version: api_version;
  correlation_id: correlation_id;
  client_id: client_id
}
type resp_header = correlation_id
type acks = int
type timeout = int
type topic = string
type partition = int
type key = bytes
type value = bytes
type record = {
  key: key;
  value: value
}
type partition_data = {
  partition: partition;
  records: record list
}
type topic_data = {
  topic: topic;
  partitions_data: partition_data list
}
type offset = int64
type node_id = int
type host = string
type port = int
type broker = {
  node_id: node_id;
  host: host;
  port: port
}
type topic_response = {
  topic: topic;
  partition: partition;
  offset: offset
}
type partition_metadata = {
  partition: partition;
  leader: node_id;
  error_code: error_code
}

module Req = struct
  module Produce = struct
    type t = {
      header: req_header;
      acks: acks;
      timeout: timeout;
      topic_data: topic_data
    }

    let create topic partition (key, value) = {
      header = {
        api_key = 0;
        api_version = 0;
        correlation_id = 1;
        client_id = "okafka.req"
      };
      acks = 1;
      timeout = 0;
      topic_data = {
        topic;
        partitions_data = [{
          partition;
          records = [{key; value}]
        }]
      }
    }
  end

  module Fetch = struct
    type t = {
      header: req_header;
      topic: topic;
      partition: partition;
      offset: offset
    }
  end

  module Metadata = struct
    type t = {
      header: req_header;
      topic: topic
    }

    let create client_id topic = {
      header = {
        api_key = 3;
        api_version = 0;
        correlation_id = 1;
        client_id
      };
      topic
    }
  end
  
  type t =
    | Produce of Produce.t
    | Fetch of Fetch.t
    | Metadata of Metadata.t

  let req_header_to_bytes header =
    let open Stdint in
    let {api_key; api_version; correlation_id; client_id} = header in
    let client_id_length = String.length header.client_id in
    to_buffer [
      write_2_bytes api_key;
      write_2_bytes api_version;
      write_4_bytes correlation_id;
      write_2_bytes client_id_length;
      client_id
    ]

  let to_message_set records =
    to_buffer (
      List.map (fun {key; value} ->
        let message = to_buffer [
          write_1_byte 0; (* magic byte *)
          write_1_byte 0; (* attributes *)
          write_4_bytes (Bytes.length key); key; (* key size and key *)
          write_4_bytes (Bytes.length value); value (* value size and value *)] in
        let message_with_crc = to_buffer [
          write_4_bytes Core.(Int63.to_int_exn (Crc.crc32 message));
          message] in
        to_buffer [
          write_8_bytes (Int64.of_int (-1)); (* offset *)
          write_4_bytes (Bytes.length message_with_crc); (* message size *)
          message_with_crc]
      ) records
    )

  let to_partition_data partition_data =
    List.map (fun {partition; records} ->
      let message_set = to_message_set records in
      to_buffer [
        write_4_bytes partition; (* partition *)
        write_4_bytes (Bytes.length message_set); (* message set size *)
        message_set]
    ) partition_data

  let produce_req_to_bytes req =
    let open Stdint in
    let {Produce.header; acks; timeout; topic_data} = req in
    let {topic; partitions_data} = topic_data in
    let partition_data = to_partition_data partitions_data in
    let partition_data_size = List.length partition_data in
    let topic_length = String.length topic_data.topic in
    let req = to_buffer [
      req_header_to_bytes header;
      write_2_bytes acks;
      write_4_bytes timeout;
      write_4_bytes 1; (* length of [topic_data] *)
      write_2_bytes topic_length;
      topic;
      write_4_bytes partition_data_size;
      to_buffer partition_data
    ] in
    to_buffer [
      write_4_bytes (Bytes.length req);
      req
    ]

  let fetch_req_to_bytes req =
    let open Stdint in
    let {Fetch.header; topic; partition; offset} = req in
    let topic_length = String.length topic in
    let req = to_buffer [
      req_header_to_bytes header;
      write_4_bytes (-1); (* replica_id *)
      write_4_bytes 1000; (* max_wait_time *)
      write_4_bytes 0; (* min_bytes *)
      write_4_bytes 1; (* length of [topics] *)
      write_2_bytes topic_length;
      topic;
      write_4_bytes 1; (* length of [partitions] *)
      write_4_bytes partition;
      write_8_bytes offset;
      write_4_bytes 100000 (* max_bytes per partition *)
    ] in
    to_buffer [
      write_4_bytes (Bytes.length req);
      req
    ]

  let metadata_req_to_bytes req =
    let open Stdint in
    let {Metadata.header; topic} = req in
    let topic_length = String.length topic in
    let req = to_buffer [
      req_header_to_bytes header;
      write_4_bytes 1; (* length of [topic] *)
      write_2_bytes topic_length;
      topic
    ] in
    to_buffer [
      write_4_bytes (Bytes.length req);
      req
    ]

  let encode = function
    | Produce r ->
        produce_req_to_bytes r
    | Fetch r ->
        fetch_req_to_bytes r
    | Metadata r ->
        metadata_req_to_bytes r
end

module Resp = struct
  module Produce = struct
    type t = {
      correlation_id: correlation_id;
      topic_response: topic_response;
      error_code: error_code
    }
  end

  module Fetch = struct
    type t = {
      correlation_id: correlation_id;
      topic: topic;
      error_code: error_code
    }
  end

  module Metadata = struct
    type t = {
      correlation_id: correlation_id;
      brokers: broker list;
      leaders: partition_metadata list;
      error_code: error_code
    }

    let get_broker part brokers leaders =
      let {leader} = List.find (fun {partition} -> partition = part) leaders in
      let {node_id; host; port} = List.find (fun {node_id} -> node_id = leader) brokers in
      {node_id; host; port}
  end

  type t =
    | Produce of Produce.t
    | Fetch of Fetch.t
    | Metadata of Metadata.t
end

type ('a, 'b) client_response =
  | Just of 'b
  | Retry of ('a * broker)
  | Failed of string

let with_pos pos value =
  (pos, value)

let decode_produce_resp bytes =
  let (pos, correlation_id) = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int |> with_pos 4 in
  let (pos, _topic_response_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, topic_length) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
  let (pos, topic) = (Bytes.sub bytes pos topic_length) |> with_pos (pos + topic_length) in
  let (pos, _partitions_resp_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, partition) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
  let (_pos, offset) = (Bytes.sub bytes pos 8) |> read_int64 |> Int64.to_int64 |> with_pos (pos + 8) in
  let topic_response = {topic; partition; offset} in
  Resp.({
    Produce.correlation_id;
    topic_response;
    error_code
  })

let decode_fetch_resp bytes =
  let (pos, correlation_id) = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int |> with_pos 4 in
  let (pos, _responses_length) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, topic_length) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
  let (pos, topic) = (Bytes.sub bytes pos topic_length) |> with_pos (pos + topic_length) in
  let (pos, _partitions_length) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, _partition) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
  let (pos, _high_watermark) = (Bytes.sub bytes pos 8) |> read_int64 |> Int64.to_int |> with_pos (pos + 8) in
  let (pos, record_set_length) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let _pos =
    if record_set_length != 0 then
      let (pos, offset) = (Bytes.sub bytes pos 8) |> read_int64 |> Int64.to_string |> with_pos (pos + 8) in
      let (pos, message_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, crc) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, magic_byte) = (Bytes.sub bytes pos 1) |> read_int8 |> Int8.to_int |> with_pos (pos + 1) in
      let (pos, attr) = (Bytes.sub bytes pos 1) |> read_int8 |> Int8.to_int |> with_pos (pos + 1) in
      let (pos, key_length) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, key) = (Bytes.sub bytes pos key_length) |> with_pos (pos + key_length) in
      let (pos, val_length) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, value) = (Bytes.sub bytes pos val_length) |> with_pos (pos + val_length) in
      let _ = Lwt_io.printlf "Recordset length: %d, offset: %s, message size: %d, crc: %d, magic: %d, attr: %d, key: %s, val: %s"
      record_set_length offset message_size crc magic_byte attr key value in
      pos
    else
      let _ = Lwt_io.printlf "Recorset length: %d" record_set_length in
      pos in
  Resp.({
    Fetch.correlation_id;
    topic;
    error_code
  })

let decode_metadata_resp bytes =
  let (pos, correlation_id) = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int |> with_pos 4 in
  let (pos, brokers_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let rec brokers n _pos acc =
    if n = 0 then
      (_pos, acc)
    else (
      let pos = _pos in
      let (pos, node_id) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, host_length) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
      let (pos, host) = Bytes.sub bytes pos host_length |> with_pos (pos + host_length) in
      let (pos, port) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      brokers (n - 1) pos ({node_id; host; port} :: acc)
    ) in
  let (pos, brokers) = brokers brokers_size 8 [] in
  let (pos, _topic_metadata_length) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
  let (pos, _topic_length) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
  let (pos, _topic) = (Bytes.sub bytes pos _topic_length) |> with_pos (pos + _topic_length) in
  let (pos, part_metadata_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
  let rec part_metadata n _pos acc =
    if n = 0 then
      (_pos, acc)
    else (
      let pos = _pos in
      let (pos, error_code) = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int |> with_pos (pos + 2) in
      let (pos, partition) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, leader) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, replica_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, _replicas) = (Bytes.sub bytes pos (replica_size * 4)) |> read_int32 |> Int32.to_int |> with_pos (pos + (replica_size * 4)) in
      let (pos, isr_size) = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int |> with_pos (pos + 4) in
      let (pos, _isr) = (Bytes.sub bytes pos (isr_size * 4)) |> read_int32 |> Int32.to_int |> with_pos (pos + (isr_size * 4)) in
      part_metadata (n - 1) pos ({partition; leader; error_code} :: acc)
    ) in
  let (_pos, leaders) = part_metadata part_metadata_size pos [] in
  Resp.({
    Metadata.correlation_id;
    brokers;
    leaders;
    error_code
  })

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

(*let parse_api_versions_resp ic =*)
(*  let open Lwt in*)
(*  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=*)
(*  read_bytes ic >|= decode_api_versions_resp*)

let parse_fetch_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=
  read_bytes ic >|= decode_fetch_resp

let parse_metadata_resp ic =
  let open Lwt in
  Lwt_io.read ~count:4 ic >|= read_int32 >|= Int32.to_int >>=
  read_bytes ic >|= decode_metadata_resp
