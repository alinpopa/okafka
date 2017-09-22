include Core.Caml.Bytes
include Stdint

let to_buffer data =
  List.fold_left (fun acc e -> cat acc e) (create 0) data

let write_1_byte (i : int) =
  let b = create 1 in
  let _ = Int8.(to_bytes_big_endian (of_int i) b 0) in
  b

let write_2_bytes (i : int) =
  let b = create 2 in
  let _ = Int16.(to_bytes_big_endian (of_int i) b 0) in
  b

let write_4_bytes (i : int) =
  let b = create 4 in
  let _ = Int32.(to_bytes_big_endian (of_int i) b 0) in
  b

let write_8_bytes (i : int64) =
  let b = create 8 in
  let _ = Int64.(to_bytes_big_endian (of_int64 i) b 0) in
  b

let read_int8 buffer =
  Int8.of_bytes_big_endian buffer 0

let read_int16 buffer =
  Int16.of_bytes_big_endian buffer 0

let read_int32 buffer =
  Int32.of_bytes_big_endian buffer 0

let read_int64 buffer =
  Int64.of_bytes_big_endian buffer 0
