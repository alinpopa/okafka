type t = (Lwt_io.input_channel * Lwt_io.output_channel)

val create : string -> int -> t Lwt.t

val close : t -> unit Lwt.t
