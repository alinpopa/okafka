let () =
  let open Lwt in
  let open Okafka.Client in
  let open Okafka.Protocol in
  let open Okafka.Bytes in
  let open Okafka.Lib in
  let open Stdint in
  Lwt_main.run (
    let buff_metadata = encode_req (create_metadata_req "simple_client" "logging") in
    let buff1 = encode_req (create_produce_req "simple_client" "logging" 0 "some_key" "my very value") in
    (*let buff11 = encode_req (create_metadata_req "simple_client" "logging") in*)
    (*let buff2 = encode_req (create_api_versions_req "simple_client") in*)
    (*let buff3 = encode_req (create_fetch_req "simple_client" "logging" 0 (Int64.of_int 0)) in*)
    let default_ctx = Conduit_lwt_unix.default_ctx in
    let connection =
      Conduit_lwt_unix.endp_to_client
        ~ctx:default_ctx
        Conduit.(`TCP ((Ipaddr.of_string_exn "127.0.0.1"), 19092)) in
    let connected_client =
      connection >>= fun client ->
      Conduit_lwt_unix.connect default_ctx client in
    connected_client >>= fun ((flow, ic, oc)) ->
      Lwt_io.write oc buff_metadata >>=
      fun _ -> parse_metadata_resp ic >|=
      (fun x -> match x with
        | Left x -> x
        | Right x -> response_to_string x) >>=
      Lwt_io.printl >>=
      fun _ -> Lwt_io.write oc buff1 >>=
      fun _ -> parse_produce_resp ic >|=
      (fun x -> match x with
        | Left x -> x
        | Right x -> response_to_string x) >>=
      Lwt_io.printl
      (*fun _ -> parse_metadata_resp ic >>=*)
      (*fun _ -> Lwt.return ()*)
      (*fun _ -> Lwt_io.write oc buff2 >>=*)
      (*fun _ -> parse_api_versions_resp ic >|=*)
      (*(fun x -> match x with*)
      (*  | Left x -> x*)
      (*  | Right x -> response_to_string x) >>=*)
      (*Lwt_io.printl >>=*)
      (*fun _ -> Lwt_io.write oc buff3 >>=*)
      (*fun _ -> parse_fetch_resp ic >|=*)
      (*(fun x -> match x with*)
      (*  | Left x -> x*)
      (*  | Right x -> response_to_string x) >>=*)
      (*Lwt_io.printl*)
  )
