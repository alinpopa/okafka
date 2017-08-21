open Okafka

let () =
  let open Lwt in
  Lwt_main.run (
    let buff = kafka_api_versions_req () in
    let default_ctx = Conduit_lwt_unix.default_ctx in
    let connection =
      Conduit_lwt_unix.endp_to_client
        ~ctx:default_ctx
        Conduit.(`TCP ((Ipaddr.of_string_exn "192.168.1.103"), 19091)) in
    let connected_client =
      connection >>= fun client ->
      Conduit_lwt_unix.connect default_ctx client in
    let r = connected_client >>= fun ((flow, ic, oc)) -> Lwt_io.write oc buff >>= fun _ -> response_parser ic in
    response_printer r
  )
