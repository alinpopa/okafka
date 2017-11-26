let () =
  let open Lwt in
  let open Okafka in
  let producer = Producer.create (Producer.broker ("127.0.0.1", 19091)) "logging" 0 in
  Lwt_main.run (
    Producer.send ~msg:(Bytes.of_string "one key", Bytes.of_string "one value") ~producer >>=
    function
    | Producer.Response (error_code, _) ->
        Lwt_io.printlf "Error code: %d" error_code
    | Producer.Reconnected (_, (error_code, _)) ->
        Lwt_io.printlf "Error code: %d" error_code
  )
