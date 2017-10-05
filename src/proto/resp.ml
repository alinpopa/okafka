open Proto

module Produce = struct
  type t = {
    correlation_id: correlation_id;
    topic_response: topic_response;
    error_code: error_code
  }
end

module Fetch = struct
  type t = {
    correlation_id: correlation_id;
    topic: topic;
    error_code: error_code
  }
end

module Metadata = struct
  type t = {
    correlation_id: correlation_id;
    brokers: broker list;
    leaders: partition_metadata list;
    error_code: error_code
  }

  let get_broker part brokers leaders =
    let {leader} = List.find (fun {partition} -> partition = part) leaders in
    let {node_id; host; port} = List.find (fun {node_id} -> node_id = leader) brokers in
    {node_id; host; port}
end

type t =
  | Produce of Produce.t
  | Fetch of Fetch.t
  | Metadata of Metadata.t
