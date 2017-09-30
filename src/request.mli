module type Pipe = sig
  type request
  type response
end

module Make :
  functor (From : sig
    type req
    type resp
  end) -> Pipe
    with type request = From.req
    with type response = From.resp
