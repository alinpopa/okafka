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

let send ~producer ~topic ~partition ~msg =
  let open Lwt in
  producer >>=
  fun (ic, oc) -> Lwt_io.write oc "ok" >|=
  (*fun _ -> Lwt_io.read ~count:1 ic >|=*)
  (*fun _ -> parse_produce_resp ic >|=*)
  fun _ -> Response (0, 0)
