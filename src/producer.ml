open Proto

type t = (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

type 'a resp =
    | Response of 'a
    | Reconnected of (t * 'a)

type 'a lwt = 'a resp Lwt.t

type broker = (string * int)

let broker (host, port) =
  (host, port)

let create (host, port) =
  let open Lwt in
  let default_ctx = Conduit_lwt_unix.default_ctx in
  let connect ip port = (
    let connection =
      Conduit_lwt_unix.endp_to_client
      ~ctx:default_ctx
      Conduit.(`TCP ((Ipaddr.of_string_exn ip), port)) in
    let connected_client =
      connection >>=
      fun client -> Conduit_lwt_unix.connect default_ctx client in
    connected_client >|=
    (fun ((_flow, ic, oc)) -> (ic, oc))
  ) in
  connect host port

let send_req reader writer (req : Req.Produce.t) partition =
  let open Lwt in
  let client_id = req.header.client_id in
  let buff = Req.encode (Req.Produce req) in
  Lwt_io.write writer buff >>=
  fun _ -> parse_produce_resp reader >>=
  fun ({Resp.Produce.topic_response; error_code} as resp) ->
  if error_code != 0 then
    let buff_meta =
      let req = Req.Metadata.create client_id topic_response.topic in
      Req.encode (Req.Metadata req) in
    Lwt_io.write writer buff_meta >>=
    fun _ -> parse_metadata_resp reader >|=
    fun ({Resp.Metadata.correlation_id; brokers; leaders}) ->
      Retry (req, Resp.Metadata.get_broker partition brokers leaders)
  else
    Lwt.return (Just resp)

let send ~producer ~topic ~partition ~msg =
  let open Lwt in
  let req = Req.Produce.create topic partition msg in
  let rec send producer req reconnected =
    producer >>=
    fun (ic, oc) ->
      send_req ic oc req partition >>=
      function
        | Just ({Resp.Produce.error_code}) ->
            if reconnected = true then
              Lwt.return (Reconnected (producer, (error_code, error_code)))
            else
              Lwt.return (Response (error_code, error_code))
        | Retry (req, {host; port}) ->
            send (create (host, port)) req true
        | Failed err ->
            Lwt.return (Response (0, 0)) in
  send producer req false
