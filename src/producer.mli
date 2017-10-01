type t
type broker
type 'a resp =
  | Response of 'a
  | Reconnected of (t * 'a)
type 'a lwt = 'a resp Lwt.t

val broker : (string * int) -> broker

val create : broker -> t

val send :
  producer:t ->
  topic:string ->
  partition:int ->
  msg:(bytes * bytes) ->
  (int * int) lwt
