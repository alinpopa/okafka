type ('s, 'a) state = ('s -> 'a * 's)

let return2 f =
  f

let return a =
  fun s -> (a, s)

let run s x =
  s x

let (>>>) m f =
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
  get >>> fun x ->
  put (f x)
