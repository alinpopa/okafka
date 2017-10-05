type t
type broker
type 'a resp =
  | Response of 'a
  | Reconnected of (t * 'a)
type 'a lwt = 'a resp Lwt.t

val broker : (string * int) -> broker

val create : broker -> string -> int -> t

val send :
  producer:t ->
  msg:(bytes * bytes) ->
  (int * int) lwt
