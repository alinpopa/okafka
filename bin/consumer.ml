open Lwt
open Okafka.Client
open Okafka.Proto
open Okafka_lib.Bytes
open Okafka_lib.Lib
open Stdint

let resp_fetch_to_string resp =
  let {Resp.Fetch.correlation_id = corr_id; topic; error_code = err_code} = resp in
  Printf.(
    let a = sprintf "\t[corr_id:%d]" corr_id in
    let b = sprintf "\t[topic:%s]" topic in
    let c = sprintf "\t[err_code:%d]" err_code in
    sprintf "FetchResponse\n%s\n%s\n%s"
    a b c)

let get_broker part brokers leaders =
  let {leader} = List.find (fun {partition} -> partition = part) leaders in
  let {node_id; host; port} = List.find (fun {node_id} -> node_id = leader) brokers in
  {node_id; host; port}

let send_req (reader, writer) (req : Req.Fetch.t) =
  let (client_id, partition) = (req.header.client_id, req.partition) in
  let buff = Req.encode (Req.Fetch req) in
  Lwt_io.write writer buff >>=
  fun _ -> parse_fetch_resp reader >>=
  fun ({Resp.Fetch.correlation_id; topic; error_code} as resp) ->
  if error_code != 0 then
    let buff_meta =
      let req = Req.Metadata.create client_id topic in
      Req.encode (Req.Metadata req) in
    Lwt_io.write writer buff_meta >>=
    fun _ -> parse_metadata_resp reader >|=
    fun ({Resp.Metadata.correlation_id; brokers; leaders}) ->
      Retry (req, get_broker partition brokers leaders)
  else
    Lwt.return (Just resp)

let () =
  Lwt_main.run (
    (*let _buff = encode_req (create_fetch_req "simple_client" "logging" 0 (Int64.of_int 0)) in*)
    (*let _buff_meta = encode_req (create_metadata_req "simple_client" "logging") in*)
    let default_ctx = Conduit_lwt_unix.default_ctx in
    let rec send_request ip port req = (
      let connection =
        Conduit_lwt_unix.endp_to_client
        ~ctx:default_ctx
        Conduit.(`TCP ((Ipaddr.of_string_exn ip), port)) in
      let connected_client =
        connection >>=
        fun client -> Conduit_lwt_unix.connect default_ctx client in
      connected_client >>=
      fun ((_flow, ic, oc)) -> send_req (ic, oc) req >>=
      function
        | Just resp ->
            Lwt.return (resp_fetch_to_string resp) >>= Lwt_io.printl
        | Retry (req, {host; port}) ->
            send_request host port req
        | Failed err ->
            Lwt.return err >>=
      Lwt_io.printl
    ) in
    (*let req = ((1, 0, 1, "simple_client"), "logging", 0, (Int64.of_int 0)) in*)
    let req = {Req.Fetch.header = {api_key = 1; api_version = 0; correlation_id = 1; client_id = "simple_client"};
    topic = "logging"; partition = 0; offset = (Int64.of_int 0)} in
    send_request "127.0.0.1" 19093 req
  )
