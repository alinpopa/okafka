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

type request =
  | ApiVersionsReq of req_header
  | ProduceReq of (req_header * acks * timeout * topic_data)

type response =
  | ApiVersionsResponse of (correlation_id * error_code * api_versions list)
  | HeadersResponse of (correlation_id * error_code)
  | ProduceResponse of (correlation_id * int)

let create_api_versions_req client_id =
  ApiVersionsReq (18, 0, 1, client_id)

let create_produce_req client_id topic partition key value =
  ProduceReq ((0, 0, 1, client_id), 1, 0, (topic, [(partition, [(key, value)])]))

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
        write_8_bytes (-1); (* offset *)
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

let request_header api_key api_vsn corr_id client_id =
  let client_id_length = Bytes.length client_id in
  to_buffer [
    write_2_bytes api_key;
    write_2_bytes api_vsn;
    write_4_bytes corr_id;
    write_2_bytes client_id_length;
    client_id
  ]

let encode_req = function
  | ApiVersionsReq (api_key, api_vsn, corr_id, client_id) ->
      let req = request_header api_key api_vsn corr_id client_id in
      to_buffer [
        write_4_bytes (Bytes.length req);
        req
      ]
  | ProduceReq (
    (api_key,
     api_version,
     correlation_id,
     client_id),
    acks, timeout, topic_data) ->
      let (topic, partition_data) = topic_data in
      let partition_data = to_partition_data partition_data in
      let partition_data_size = List.length partition_data in
      let topic_length = String.length topic in
      let req = to_buffer [
        request_header api_key api_version correlation_id client_id;
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

let decode_produce_resp bytes =
  try
    let corr_id = (Bytes.sub bytes 0 4) |> read_int32 in
    let topic_data_size = (Bytes.sub bytes 4 4) |> read_int32 in
    Right (ProduceResponse (Int32.to_int corr_id,
                            Int32.to_int topic_data_size))
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
