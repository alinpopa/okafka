open OkafkaLib.Bytes
open OkafkaProto

let req_header_to_bytes header =
  let open Stdint in
  let open Proto in
  let {api_key; api_version; correlation_id; client_id} = header in
  let client_id_length = String.length header.client_id in
  to_buffer [
    write_2_bytes api_key;
    write_2_bytes api_version;
    write_4_bytes correlation_id;
    write_2_bytes client_id_length;
    (Bytes.of_string client_id)
  ]

let to_message_set records =
  let open Proto in
  to_buffer (
    List.map (fun {key; value} ->
      let message = to_buffer [
        write_1_byte 0; (* magic byte *)
        write_1_byte 0; (* attributes *)
        write_4_bytes (Bytes.length key); key; (* key size and key *)
        write_4_bytes (Bytes.length value); value (* value size and value *)] in
      let message_with_crc = to_buffer [
        write_4_bytes (Int32.to_int (Int32.of_int32 (Crc.Crc32.string (Bytes.to_string message) 0 (Bytes.length message))));
        message] in
      to_buffer [
        write_8_bytes (Int64.of_int (-1)); (* offset *)
        write_4_bytes (Bytes.length message_with_crc); (* message size *)
        message_with_crc]
    ) records
  )

let to_partition_data partition_data =
  let open Proto in
  List.map (fun {partition; records} ->
    let message_set = to_message_set records in
    to_buffer [
      write_4_bytes partition; (* partition *)
      write_4_bytes (Bytes.length message_set); (* message set size *)
      message_set]
  ) partition_data

let produce_req_to_bytes req =
  let open Stdint in
  let open Proto in
  let {Req.Produce.header; acks; timeout; topic_data} = req in
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
    (Bytes.of_string topic);
    write_4_bytes partition_data_size;
    to_buffer partition_data
  ] in
  to_buffer [
    write_4_bytes (Bytes.length req);
    req
  ]

let fetch_req_to_bytes req =
  let open Stdint in
  let {Req.Fetch.header; topic; partition; offset} = req in
  let topic_length = String.length topic in
  let req = to_buffer [
    req_header_to_bytes header;
    write_4_bytes (-1); (* replica_id *)
    write_4_bytes 1000; (* max_wait_time *)
    write_4_bytes 0; (* min_bytes *)
    write_4_bytes 1; (* length of [topics] *)
    write_2_bytes topic_length;
    (Bytes.of_string topic);
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
  let {Req.Metadata.header; topic} = req in
  let topic_length = String.length topic in
  let req = to_buffer [
    req_header_to_bytes header;
    write_4_bytes 1; (* length of [topic] *)
    write_2_bytes topic_length;
    (Bytes.of_string topic)
  ] in
  to_buffer [
    write_4_bytes (Bytes.length req);
    req
  ]

let encode = function
  | Req.Produce r ->
      produce_req_to_bytes r
  | Req.Fetch r ->
      fetch_req_to_bytes r
  | Req.Metadata r ->
      metadata_req_to_bytes r
