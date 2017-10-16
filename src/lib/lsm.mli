type ('s, 'a) t

val state : ('s -> 'a * 's) -> ('s, 'a) t

val return : 'a -> ('s, 'a) t

val run : ('s, 'a) t -> 's -> ('a * 's)

val (>>=) : ('s, 'a) t -> ('a -> ('s, 'b) t) -> ('s, 'b) t

val get : ('s, 's) t

val put : 's -> ('s, unit) t

val modify : ('s -> 's) -> ('s, unit) t
