module type Pipe = sig
  type request
  type response
end

module Make(From : sig
  type req
  type resp
end) = struct
  type request = From.req
  type response = From.resp
end
