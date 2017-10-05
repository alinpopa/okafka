type ('a, 'b) either = Left of 'a | Right of 'b

let explode s =
  let rec exp i l =
    if i < 0 then l else exp (i - 1) (s.[i] :: l) in
  exp (String.length s - 1) []

let try_with f =
    let ok_result = fun result -> Lwt.return (Right result) in
    let fail_result = fun exn' -> Lwt.return (Left exn') in
    Lwt.try_bind f ok_result fail_result

let close_chan chan () =
  Lwt_io.close chan

let maybe_fail_with_log = function
  | Right r -> Lwt.return r
  | Left e ->
      Lwt.(
        Lwt_io.printlf "Exception raised: %s" (Printexc.to_string e) >>=
        fun _ -> Lwt.fail e
      )
