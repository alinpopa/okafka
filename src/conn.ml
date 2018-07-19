open OkafkaLib.Lib

type t = (Lwt_io.input_channel * Lwt_io.output_channel)

let create host port =
  let open Lwt in
  (*let open Conduit in*)
  let default_ctx = Conduit_lwt_unix.default_ctx in
  let connection =
    Conduit_lwt_unix.endp_to_client
      ~ctx:default_ctx
      (`TCP ((Ipaddr.of_string_exn host), port)) in
  let connected_client =
    connection >>= fun client -> Conduit_lwt_unix.connect ~ctx:default_ctx client in
  connected_client >|=
    fun ((_flow, ic, oc)) -> (ic, oc)

let close (ic, oc) =
  let open Lwt in
  try_with (close_chan ic) >>=
  fun _ -> try_with (close_chan oc) >>=
  fun _ -> Lwt.return ()
