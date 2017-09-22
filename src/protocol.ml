open Bytes
open Lib

type correlation_id = int
type client_id = string
type api_version = int
type api_key = int
type error_code = int
type api_versions = (api_key * int * int)
type req_header = (api_key * api_version * correlation_id * client_id)
type resp_header = correlation_id
type acks = int
type timeout = int
type topic = string
type partition = int
type key = bytes
type value = bytes
type record = (key * value)
type partition_data = (partition * record list)
type topic_data = (topic * partition_data list)
type offset = int64
type node_id = int
type host = string
type port = int
type broker = (node_id * host * port)
type partition_response = (partition * error_code * offset)
type topic_response = (topic * partition_response list)

type request =
  | ApiVersionsReq of req_header
  | ProduceReq of (req_header * acks * timeout * topic_data)
  | FetchReq of (req_header * topic * partition * offset)
  | MetadataReq of (req_header * topic)

type response =
  | ApiVersionsResponse of (correlation_id * error_code * api_versions list)
  | ProduceResponse of (correlation_id * topic_response list)
  | FetchResponse of (correlation_id * topic)
  | MetadataResponse of (correlation_id * broker list)

let create_api_versions_req client_id =
  ApiVersionsReq (18, 0, 1, client_id)

let create_produce_req client_id topic partition key value =
  ProduceReq ((0, 0, 1, client_id), 1, 0, (topic, [(partition, [(key, value)])]))

let create_fetch_req client_id topic partition offset =
  FetchReq ((1, 0, 1, client_id), topic, partition, offset)

let create_metadata_req client_id topic =
  MetadataReq ((3, 0, 1, client_id), topic)

let to_message_set records =
  to_buffer (
    List.map (fun (key, value) ->
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
  List.map (fun (partition, records) ->
    let message_set = to_message_set records in
    to_buffer [
      write_4_bytes partition; (* partition *)
      write_4_bytes (Bytes.length message_set); (* message set size *)
      message_set]
  ) partition_data

let req_header_to_bytes header =
  let open Stdint in
  let (api_key, api_version, correlation_id, client_id) = header in
  let client_id_length = String.length client_id in
  to_buffer [
    write_2_bytes api_key;
    write_2_bytes api_version;
    write_4_bytes correlation_id;
    write_2_bytes client_id_length;
    client_id
  ]

let api_versions_req_to_bytes req =
  let open Stdint in
  let header = req in
  let req = to_buffer [
    req_header_to_bytes header
  ] in
  to_buffer [
    write_4_bytes (Bytes.length req);
    req
  ]

let produce_req_to_bytes req =
  let open Stdint in
  let (header, acks, timeout, topic_data) = req in
  let (topic, partition_data) = topic_data in
  let partition_data = to_partition_data partition_data in
  let partition_data_size = List.length partition_data in
  let topic_length = String.length topic in
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
  let (header, topic, partition, offset) = req in
  let topic_length = String.length topic in
  let req = to_buffer [
    req_header_to_bytes header;
    write_4_bytes (-1); (* replica_id *)
    write_4_bytes 5000; (* max_wait_time *)
    write_4_bytes 2048; (* min_bytes *)
    write_4_bytes 1; (* length of [topics] *)
    write_2_bytes topic_length;
    topic;
    write_4_bytes 1; (* length of [partitions] *)
    write_4_bytes partition;
    write_8_bytes offset;
    write_4_bytes 1048576 (* max_bytes per partition *)
  ] in
  to_buffer [
    write_4_bytes (Bytes.length req);
    req
  ]

let metadata_req_to_bytes req =
  let open Stdint in
  let (header, topic) = req in
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

let encode_req = function
  | ApiVersionsReq req ->
      api_versions_req_to_bytes req
  | ProduceReq req ->
      produce_req_to_bytes req
  | FetchReq req ->
      fetch_req_to_bytes req
  | MetadataReq req ->
      metadata_req_to_bytes req

let decode_produce_resp bytes =
  try
    let correlation_id = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int in
    let topic_response_size = (Bytes.sub bytes 4 4) |> read_int32 |> Int32.to_int in
    let rec topic_response n pos acc =
      if n = 0 then
        acc
      else (
        let topic_length = (Bytes.sub bytes pos 2) |> read_int16 |> Int16.to_int in
        let topic = Bytes.sub bytes (pos + 2) topic_length in
        let partitions_size = (Bytes.sub bytes (pos + 2 + topic_length) 4) |> read_int32 |> Int32.to_int in
        let rec partitions n pos acc =
          if n = 0 then
            (pos, acc)
          else (
            let partition = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int in
            let error_code = (Bytes.sub bytes (pos + 4) 2) |> read_int16 |> Int16.to_int in
            let offset = (Bytes.sub bytes (pos + 4 + 2) 8) |> read_int64 in
            let new_position = pos + 4 + 2 + 8 in
            partitions (n - 1) new_position ((partition, error_code, offset) :: acc)
          ) in
        let (pos, partitions) = partitions partitions_size (pos + 2 + topic_length + 4) [] in
        let data = ((Bytes.to_string topic), partitions) in
        topic_response (n - 1) pos (data :: acc)
      ) in
    let topic_response = topic_response topic_response_size 8 [] in
    Right (ProduceResponse (correlation_id, topic_response))
  with
  _ ->
    Left "Error while decoding produce response"

let decode_api_versions_resp bytes =
  try
    let corr_id = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int in
    let error_code = (Bytes.sub bytes 4 2) |> read_int16 |> Int16.to_int in
    let versions_size = (Bytes.sub bytes 6 4) |> read_int32 |> Int32.to_int in
    let rec versions n pos acc =
      if n = 0 then acc
      else versions (n - 1) (n + 6) (((Bytes.sub bytes pos 2 |> read_int16 |> Int16.to_int),
            (Bytes.sub bytes (pos + 2) 2 |> read_int16 |> Int16.to_int),
            (Bytes.sub bytes (pos + 4) 2 |> read_int16 |> Int16.to_int)) :: acc) in
    Right (ApiVersionsResponse (corr_id,
                                error_code,
                                versions versions_size 10 []))
  with
  _ ->
    Left "Error while decoding api versions response"

let decode_fetch_resp bytes =
  try
    let correlation_id = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int in
    let _ = Lwt_io.printlf "Response size: %d" (Bytes.length bytes) in
    (*let x = (Bytes.sub bytes 4 4) |> read_int32 |> Int32.to_int in*)
    (*let _ = Lwt_io.printlf "X: %d" x in*)
    (*let topic_length = (Bytes.sub bytes 8 2) |> read_int16 |> Int16.to_int in*)
    (*let topic = (Bytes.sub bytes 10 topic_length) in*)
    Right (FetchResponse (correlation_id, "logging"))
  with
  _ ->
    Left "Error while decoding fetch response"

let decode_metadata_resp bytes =
  try
    let correlation_id = (Bytes.sub bytes 0 4) |> read_int32 |> Int32.to_int in
    let brokers_size = (Bytes.sub bytes 4 4) |> read_int32 |> Int32.to_int in
    let rec brokers n pos acc =
      if n = 0 then
        (pos, acc)
      else (
        let node_id = (Bytes.sub bytes pos 4) |> read_int32 |> Int32.to_int in
        let host_length = (Bytes.sub bytes (pos + 4) 2) |> read_int16 |> Int16.to_int in
        let host = Bytes.sub bytes (pos + 4 + 2) host_length in
        let port = (Bytes.sub bytes (pos + 4 + 2 + host_length) 4) |> read_int32 |> Int32.to_int in
        let last_pos = 10 + host_length in
        brokers (n - 1) (pos + last_pos) ((node_id, host, port) :: acc)
      ) in
    let (_, brokers) = brokers brokers_size 8 [] in
    Right (MetadataResponse (correlation_id, brokers))
  with
  _ ->
    Left "Error while decoding metadata response"
