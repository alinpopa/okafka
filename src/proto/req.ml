open Okafka_lib.Bytes

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
