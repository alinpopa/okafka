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
    partition: partition;
    error_code: error_code;
    data: (offset * record) list
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
    let {leader; _} = List.find (fun {partition; _} -> partition = part) leaders in
    let {node_id; host; port; _} = List.find (fun {node_id; _} -> node_id = leader) brokers in
    {node_id; host; port}
end
