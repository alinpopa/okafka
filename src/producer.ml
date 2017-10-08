open OkafkaProto
open OkafkaLib.Lib
open OkafkaCodec

type state = {
  channels: (Lwt_io.input_channel * Lwt_io.output_channel);
  partition: int;
  topic: string
}

type t = state Lwt.t

type 'a resp =
    | Response of 'a
    | Reconnected of (t * 'a)

type 'a lwt = 'a resp Lwt.t

type broker = (string * int)

let broker (host, port) =
  (host, port)

let send_meta_req writer reader topic =
  let open Lwt in
  let buff_meta =
    let req = Req.Metadata.create topic in
    Encoder.encode (Req.Metadata req) in
  Lwt_io.write writer buff_meta >>=
  fun _ -> Decoder.parse_metadata_resp reader

let rec reconnect (host, port) topic partition init =
  let open Lwt in
  let connect ip port = (
    Conn.create ip port >>=
    fun ((ic, oc) as conn) ->
      if init = true then
        send_meta_req oc ic topic >>=
        fun ({Resp.Metadata.brokers; leaders}) ->
          let {Proto.host; port} = Resp.Metadata.get_broker partition brokers leaders in
          Conn.close conn >>=
          fun _ -> reconnect (host, port) topic partition false
      else
        Lwt.return {
          channels = (ic, oc);
          partition;
          topic
        }
  ) in
  try_with (fun _ -> connect host port) >>= maybe_fail_with_log

let create (host, port) topic partition =
  let open Lwt in
  reconnect (host, port) topic partition true

let send_req reader writer (req : Req.Produce.t) partition =
  let open Lwt in
  let buff = Encoder.encode (Req.Produce req) in
  Lwt_io.write writer buff >>=
  fun _ -> Decoder.parse_produce_resp reader >>=
  fun ({Resp.Produce.topic_response; error_code} as resp) ->
  if error_code = 3 || error_code = 6 then
    send_meta_req writer reader topic_response.topic >|=
    fun ({Resp.Metadata.correlation_id; brokers; leaders}) ->
      Proto.Retry (req, Resp.Metadata.get_broker partition brokers leaders)
  else
    Lwt.return (Proto.Just resp)

let send ~producer ~msg =
  let open Lwt in
  let rec send producer reconnected =
    producer >>=
    fun ({channels = (ic, oc); topic; partition}) ->
      let req = Req.Produce.create topic partition msg in
      send_req ic oc req partition >>=
      function
        | Just ({Resp.Produce.error_code}) ->
            if reconnected = true then
              Lwt.return (Reconnected (producer, (error_code, error_code)))
            else
              Lwt.return (Response (error_code, error_code))
        | Retry (req, {host; port}) ->
            send (reconnect (host, port) topic partition false) true
        | Failed err ->
            Lwt.return (Response (0, 0)) in
  send producer false
