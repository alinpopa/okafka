open Proto

module Produce : sig
  type t = {
    correlation_id: correlation_id;
    topic_response: topic_response;
    error_code: error_code
  }
end

module Fetch : sig
  type t = {
    correlation_id: correlation_id;
    topic: topic;
    partition: partition;
    error_code: error_code;
    data: (offset * record) list
  }
end

module Metadata : sig
  type t = {
    correlation_id: correlation_id;
    brokers: broker list;
    leaders: partition_metadata list;
    error_code: error_code
  }
  val get_broker : partition -> broker list -> partition_metadata list -> broker
end

type t
