(* Little State Monad *)
type ('s, 'a) state
(*type ('s, 'a) state = State of ('s -> 'a * 's)*)

val return2 : ('s -> 'a * 's) -> ('s, 'a) state

val return : 'a -> ('s, 'a) state

val run : ('a, 's) state -> 'a -> ('s * 'a)

val (>>>) : ('a, 'b) state -> ('b -> ('a, 'c) state) -> ('a, 'c) state

val get : ('s, 's) state

val put : 's -> ('s, unit) state

val modify : ('s -> 's) -> ('s, unit) state
