open Proto

module Produce : sig
  type t = {
    header: req_header;
    acks: acks;
    timeout: timeout;
    topic_data: topic_data
  }
  val create : topic -> partition -> (key * value) -> t
end

module Fetch : sig
  type t = {
    header: req_header;
    topic: topic;
    partition: partition;
    offset: offset
  }
end

module Metadata : sig
  type t = {
    header: req_header;
    topic: topic
  }
  val create : topic -> t
end

type t =
  | Produce of Produce.t
  | Fetch of Fetch.t
  | Metadata of Metadata.t
