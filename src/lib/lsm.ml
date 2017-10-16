(* a Little State Monad *)
type ('s, 'a) t = ('s -> 'a * 's)

let state f =
  f

let return x =
  fun s -> (x, s)

let run s x =
  s x

let (>>=) m f =
  fun s ->
    let (a, s') = run m s in
    run (f a) s'

let get =
  fun s ->
    (s, s)

let put x =
  fun s ->
    ((), x)

let modify f =
  get >>= fun x ->
  put (f x)
