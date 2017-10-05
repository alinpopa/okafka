open Proto

module Produce = struct
  type t = {
    header: req_header;
    acks: acks;
    timeout: timeout;
    topic_data: topic_data
  }

  let create topic partition (key, value) = {
    header = {
      api_key = 0;
      api_version = 0;
      correlation_id = 1;
      client_id = "okafka.req"
    };
    acks = 1;
    timeout = 0;
    topic_data = {
      topic;
      partitions_data = [{
        partition;
        records = [{key; value}]
      }]
    }
  }
end

module Fetch = struct
  type t = {
    header: req_header;
    topic: topic;
    partition: partition;
    offset: offset
  }
end

module Metadata = struct
  type t = {
    header: req_header;
    topic: topic
  }

  let create topic = {
    header = {
      api_key = 3;
      api_version = 0;
      correlation_id = 1;
      client_id = "okafka.meta.client"
    };
    topic
  }
end

type t =
  | Produce of Produce.t
  | Fetch of Fetch.t
  | Metadata of Metadata.t
